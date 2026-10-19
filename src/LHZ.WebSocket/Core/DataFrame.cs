using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using LHZ.WebSocket.Enums;

namespace LHZ.WebSocket.Core
{
    /// <summary>
    /// Represents a WebSocket data frame as defined in RFC 6455.
    /// Handles masking/unmasking and header serialization.
    /// </summary>
    public struct DataFrame : IDisposable
    {
        // Frame layout (RFC 6455 Section 5.2):
        //  0                   1                   2                   3
        //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
        // +-+-+-+-+-------+-+-------------+-------------------------------+
        // |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
        // |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
        // |N|V|V|V|       |S|             |   (if payload len==126/127)   |
        // | |1|2|3|       |K|             |                               |
        // +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
        // |     Extended payload length continued, if payload len == 127  |
        // + - - - - - - - - - - - - - - - +-------------------------------+
        // |                               |Masking-key, if MASK set to 1  |
        // +-------------------------------+-------------------------------+
        // | Masking-key (continued)       |          Payload Data         |
        // +-------------------------------- - - - - - - - - - - - - - - - +
        // :                     Payload Data continued ...                :
        // + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
        // |                     Payload Data continued ...                |
        // +---------------------------------------------------------------+
        private bool _disposed = false;
        /// <summary>
        /// use uint32 as 4-byte masking key.
        /// map: [0x01, 0x02, 0x03, 0x04] -> 0x04030201
        /// </summary>
        private readonly UInt32 _maskingKey;
        /// <summary>
        /// True when the MASK bit is set. Tracked separately from <see cref="_maskingKey"/>
        /// because an all-zero key is a valid (if unlikely) key and must not be mistaken
        /// for "no masking".
        /// </summary>
        private readonly bool _masked;
        /// <summary>Header and Payload data</summary>
        private readonly byte[] _buffer;
        private readonly int _headerLength;
        private readonly int _bufferLength;

        private DataFrame(byte[] buffer, int bufferLength, int headerLength, bool FIN, bool RSV1, bool RSV2, bool RSV3, OpCode opcode, UInt32 maskingKey, bool masked)
        {
            _buffer = buffer;
            _bufferLength = bufferLength;
            _headerLength = headerLength;
            _maskingKey = maskingKey;
            _masked = masked;
            InitDataFrameHeader(ToDataFrameFlag(FIN, RSV1, RSV2, RSV3, opcode));
            ApplyMask();
        }
        private DataFrame(byte[] buffer, int bufferLength, int headerLength, byte dataFrameFlag, UInt32 maskingKey, bool masked)
        {
            _buffer = buffer;
            _bufferLength = bufferLength;
            _headerLength = headerLength;
            _maskingKey = maskingKey;
            _masked = masked;
            InitDataFrameHeader(dataFrameFlag);
            ApplyMask();
        }
        private byte ToDataFrameFlag(bool FIN, bool RSV1, bool RSV2, bool RSV3, OpCode opcode)
        {
            byte dataFrameFlag = 0;
            if (FIN)
            {
                dataFrameFlag |= 0x80;
            }
            if (RSV1)
            {
                dataFrameFlag |= 0x40;
            }
            if (RSV2)
            {
                dataFrameFlag |= 0x20;
            }
            if (RSV3)
            {
                dataFrameFlag |= 0x10;
            }
            dataFrameFlag |= (byte)opcode;
            return dataFrameFlag;
        }
        private void InitDataFrameHeader(byte dataFrameFlag)
        {
            _buffer[0] = dataFrameFlag;
            int payloadLength = _bufferLength - _headerLength;
            int curIndex = 0;
            // 64-bit extended payload length (127)
            if (payloadLength > ushort.MaxValue)
            {
                _buffer[1] = 127;
                // The payload length is an int, so the high 4 bytes of the 64-bit field are
                // always zero. They must still be written explicitly: the caller's buffer is
                // not guaranteed to be zeroed (ArrayPool.Rent does not clear it).
                _buffer[2] = 0;
                _buffer[3] = 0;
                _buffer[4] = 0;
                _buffer[5] = 0;
                _buffer[6] = (byte)((payloadLength >> 24) & 0xFF);
                _buffer[7] = (byte)((payloadLength >> 16) & 0xFF);
                _buffer[8] = (byte)((payloadLength>> 8) & 0xFF);
                _buffer[9] = (byte)((payloadLength) & 0xFF);
                curIndex = 9;
            }
            // 16-bit extended payload length (126)
            else if (payloadLength > 125)
            {
                _buffer[1] = 126;
                _buffer[2] = (byte)((payloadLength >> 8) & 0xFF);
                _buffer[3] = (byte)((payloadLength) & 0xFF);
                curIndex = 3;
            }
            // 7-bit payload length (≤125)
            else
            {
                _buffer[1] = (byte)payloadLength;
                curIndex = 1;
            }
            if (Masked)
            {
                _buffer[1] |= 0x80;
                _buffer[++curIndex] = (byte)_maskingKey;
                _buffer[++curIndex] = (byte)(_maskingKey >> 8);
                _buffer[++curIndex] = (byte)(_maskingKey >> 16);
                _buffer[++curIndex] = (byte)(_maskingKey >> 24);
            }
        }
        private void ApplyMask()
        {
            if (_masked && _maskingKey != 0)
            {
                int payloadLength = _bufferLength - _headerLength;
                var byteSpan = new Span<byte>(_buffer, _headerLength, payloadLength - (payloadLength % 4));
                var intSpan = MemoryMarshal.Cast<byte, uint>(byteSpan);
                for (int i = 0; i < intSpan.Length; i++)
                {
                    intSpan[i] ^= _maskingKey;
                }
                if (payloadLength % 4 != 0)
                {
                    int offset = _headerLength + (payloadLength - (payloadLength % 4));
                    for (int i = 0; i < payloadLength % 4; i++)
                    {
                        _buffer[offset + i] ^= (byte)(_maskingKey >> ((i % 4) << 3));
                    }
                }
            }
        }
        /// <summary>
        /// XOR-masks (or unmasks) the payload against the 4-byte key, in place.
        /// The key byte applied to each octet is chosen by the octet's index within the
        /// payload, not its index within the backing array (RFC 6455 Section 5.3).
        /// </summary>
        private static void ApplyMask(ArraySegment<byte> data, UInt32 maskingKey)
        {
            var array = data.Array;
            if (array == null || maskingKey == 0)
            {
                return;
            }
            int offset = data.Offset;
            for (int i = 0; i < data.Count; i++)
            {
                array[offset + i] ^= (byte)(maskingKey >> ((i % 4) << 3));
            }
        }
        /// <summary>
        /// Creates a frame from a raw header byte, stating explicitly whether the MASK bit was set.
        /// </summary>
        internal static DataFrame CreateDataFrame(byte[] buffer, int bufferLength, int headerLength, byte dataFrameFlag, UInt32 maskingKey, bool masked)
        {
            return new DataFrame(buffer, bufferLength, headerLength, dataFrameFlag, maskingKey, masked);
        }
        /// <summary>
        /// Creates an outgoing frame with the given opcode and payload.
        /// <paramref name="opcode"/>
        /// <paramref name="FIN"/>
        /// <paramref name="data">send data, warring: if maskingKey > 0 then this array will be masked so the value will change</paramref>
        /// <paramref name="maskingKey"/>
        /// </summary>
        public static DataFrame CreateDataFrame(OpCode opcode, bool FIN, byte[] data, UInt32 maskingKey = 0)
        {
            return CreateDataFrame(opcode, FIN, false, false, false, data, maskingKey);
        }

        /// <summary>
        /// Creates an outgoing frame with the given opcode and payload.
        /// <paramref name="opcode"/>
        /// <paramref name="FIN"/>
        /// <paramref name="data">send data, warring: if maskingKey > 0 then this array will be masked so the value will change</paramref>
        /// <paramref name="maskingKey"/>
        /// </summary>
        public static DataFrame CreateDataFrame(OpCode opcode, bool FIN, bool RSV1, bool RSV2, bool RSV3, byte[] data, UInt32 maskingKey = 0)
        {
            var headerLength = GetHeaderLength(data.Length, maskingKey != 0);
            var buffer = ArrayPool<byte>.Shared.Rent(data.Length + headerLength);
            System.Buffer.BlockCopy(data, 0, buffer, headerLength, data.Length);
            return new DataFrame(buffer, data.Length + headerLength, headerLength, FIN, RSV1, RSV2, RSV3, opcode, maskingKey, maskingKey != 0);
        }

        /// <summary>
        /// Creates an outgoing frame with the given string payload.
        /// <paramref name="text">send text, warring: if maskingKey > 0 then this array will be masked so the value will change</paramref>
        /// <paramref name="maskingKey"/>
        /// </summary>
        public static DataFrame CreateDataFrame(string text, UInt32 maskingKey = 0)
        {
            int payloadLength = Encoding.UTF8.GetByteCount(text);
            int headerLength = GetHeaderLength(payloadLength, maskingKey != 0);
            var buffer = ArrayPool<byte>.Shared.Rent(payloadLength + headerLength);
            Encoding.UTF8.GetBytes(text, 0, text.Length, buffer, headerLength);
            return new DataFrame(buffer, payloadLength + headerLength, headerLength, false, false, false, false, OpCode.Text, maskingKey, maskingKey != 0);
        }
        private static int GetHeaderLength(int payloadLength, bool masked)
        {
            int length = 2;
            if (masked)
            {
                length += 4;
            }
            // 64-bit extended payload length (127)
            if (payloadLength > ushort.MaxValue)
            {
                length += 8;
            }
            // 16-bit extended payload length (126)
            else if (payloadLength > 125)
            {
                length += 2;
            }
            return length;
        }
        /// <summary>
        /// Splits a stream into a sequence of data frames.
        /// Large payloads are fragmented across multiple continuation frames.
        /// </summary>
        /// <param name="opcode">OpCode for the first frame; subsequent frames use Continuation.</param>
        /// <param name="maskingKey">Optional 4-byte masking key.</param>
        /// <param name="data">The payload stream to read from.</param>
        /// <param name="payloadMaxLength">Max payload per frame (default 65535).</param>
        public static IEnumerable<DataFrame> CreateDataFrame(OpCode opcode, Stream data, UInt32 maskingKey = 0, int payloadMaxLength = ushort.MaxValue)
        {
            int headerLength = GetHeaderLength(payloadMaxLength, maskingKey != 0);
            byte[] buffer = ArrayPool<byte>.Shared.Rent(payloadMaxLength + headerLength);
            int readNums = 0;
            while (true)
            {
                int curReadNums = data.Read(buffer, readNums + headerLength, payloadMaxLength - readNums);
                // Read finished — emit final frame
                if (curReadNums == 0)
                {
                    // The terminating frame may have a shorter header than the one reserved for
                    // a full fragment (for example, a 0-byte payload never needs a 64-bit length).
                    int curHeaderLength = GetHeaderLength(readNums, maskingKey != 0);
                    if (readNums == 0)
                    {
                        // Nothing in this buffer: replace the full-size fragment buffer with a
                        // small one, so an empty message does not hold a payloadMaxLength array.
                        ArrayPool<byte>.Shared.Return(buffer);
                        buffer = ArrayPool<byte>.Shared.Rent(curHeaderLength);
                    }
                    else if (curHeaderLength != headerLength)
                    {
                        // Shift the payload up to the shorter header that will actually be written.
                        System.Buffer.BlockCopy(buffer, headerLength, buffer, curHeaderLength, readNums);
                    }
                    // Always emit a final frame: without it a message that ends exactly on a
                    // fragment boundary (or an empty message) would never be terminated.
                    yield return new DataFrame(buffer, readNums + curHeaderLength, curHeaderLength, true, false, false, false, opcode, maskingKey, maskingKey != 0);
                    yield break;
                }
                readNums += curReadNums;
                if (readNums == payloadMaxLength)
                {
                    // Buffer full — emit non-final fragment
                    yield return new DataFrame(buffer, readNums + headerLength, headerLength, false, false, false, false, opcode, maskingKey, maskingKey != 0);
                    opcode = OpCode.Continuation;
                    buffer = ArrayPool<byte>.Shared.Rent(payloadMaxLength + headerLength);
                    readNums = 0;
                }
            }
        }

        /// <summary>True if this is the final fragment of a message.</summary>
        public bool FIN => _buffer[0] >> 7 == 1;

        /// <summary>Reserved bit 1.</summary>
        public bool RSV1 => (_buffer[0] & 0x40) == 0x40;

        /// <summary>Reserved bit 2.</summary>
        public bool RSV2 => (_buffer[0] & 0x20) == 0x20;

        /// <summary>Reserved bit 3.</summary>
        public bool RSV3 => (_buffer[0] & 0x10) == 0x10;

        /// <summary>Frame opcode (Text, Binary, Close, Ping, Pong, Continuation).</summary>
        public OpCode Opcode => (OpCode)(_buffer[0] & 0x0F);

        /// <summary>True if the payload is masked (the MASK bit is set).</summary>
        public bool Masked => _masked;

        /// <summary>The 4-byte masking key. Meaningful only when <see cref="Masked"/> is true.</summary>
        public UInt32 MaskingKey => _maskingKey;

        /// <summary>First byte of the frame: FIN(1) + RSV1-3(3) + OpCode(4).</summary>
        public byte DataFrameFlag => _buffer[0];

        /// <summary>Get dataframe header length</summary>
        public int DataFrameHeaderLength => _headerLength;
        /// <summary>
        /// Serializes the frame header (2–14 bytes) according to RFC 6455.
        /// Supports payload lengths up to <see cref="int.MaxValue"/> (64-bit extended length).
        /// </summary>
        public Span<byte> DataFrameHeader => new Span<byte>(_buffer, 0, _headerLength);
        /// <summary>The payload data.</summary>
        public Span<byte> Data => new Span<byte>(_buffer, _headerLength, _bufferLength - _headerLength);
        /// <summary>
        /// Returns the entire frame (header + payload) as a read-only memory block.
        /// The memory is backed by the pooled buffer and is valid until the frame is disposed.
        /// </summary>
        public ReadOnlyMemory<byte> Buffer => new ReadOnlyMemory<byte>(_buffer, 0, _bufferLength);
        /// <summary>
        /// MaskingKey bytes array to uint32 
        /// </summary>
        /// <param name="bytes">MaskingKey bytes arra</param>
        /// <returns>uint32 MaskingKey</returns>
        public static UInt32 MaskingKeyToUint32(ref Span<byte> bytes)
        {
            UInt32 maskingKey = 0;
            maskingKey |= bytes[0];
            maskingKey |= ((UInt32)bytes[1]) << 8;
            maskingKey |= ((UInt32)bytes[2]) << 16;
            maskingKey |= ((UInt32)bytes[3]) << 24;
            return maskingKey;
        }
        /// <summary>
        /// Yields data frames synchronously until a close frame is encountered
        /// or cancellation is requested.
        /// </summary>
        public void Dispose()
        {
            if(!_disposed)
            {
                _disposed = true;
                ArrayPool<byte>.Shared.Return(_buffer);
            }
        }
    }
}