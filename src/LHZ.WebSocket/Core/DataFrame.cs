using System;
using System.Collections.Generic;
using System.IO;
using LHZ.WebSocket.Enums;

namespace LHZ.WebSocket.Core
{
    /// <summary>
    /// Represents a WebSocket data frame as defined in RFC 6455.
    /// Handles masking/unmasking and header serialization.
    /// </summary>
    public struct DataFrame
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

        /// <summary>First byte of the frame: FIN(1) + RSV1-3(3) + OpCode(4).</summary>
        private readonly byte _dataFrameFlag;

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

        /// <summary>Payload data segment.</summary>
        private ArraySegment<byte> _data;

        /// <summary>
        /// Creates a new outgoing frame. Applies XOR masking if a key is provided.
        /// Masking is applied in place, so the caller's buffer is modified.
        /// </summary>
        private DataFrame(bool FIN, bool RSV1, bool RSV2, bool RSV3, OpCode opcode, UInt32 maskingKey, ArraySegment<byte> data)
        {
            _maskingKey = maskingKey;
            _masked = maskingKey != 0;
            _data = data;
            if (FIN)
            {
                _dataFrameFlag |= 0x80;
            }
            if (RSV1)
            {
                _dataFrameFlag |= 0x40;
            }
            if (RSV2)
            {
                _dataFrameFlag |= 0x20;
            }
            if (RSV3)
            {
                _dataFrameFlag |= 0x10;
            }
            _dataFrameFlag |= (byte)opcode;
            ApplyMask(_data, _maskingKey);
        }

        /// <summary>
        /// Creates a frame from a pre-parsed header byte (used when reading incoming frames).
        /// Unmasking is applied in place, so the supplied buffer is modified.
        /// </summary>
        private DataFrame(byte dataFrameFlag, ArraySegment<byte> data, UInt32 maskingKey, bool masked)
        {
            _maskingKey = maskingKey;
            _masked = masked;
            _data = data;
            _dataFrameFlag = dataFrameFlag;
            ApplyMask(_data, _maskingKey);
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

        /// <summary>Creates a frame from a raw header byte (used by DataFrameReader).</summary>
        internal static DataFrame CreateDataFrame(byte dataFrameFlag, byte[] data, UInt32 maskingKey = 0)
        {
            return CreateDataFrame(dataFrameFlag, data, maskingKey, maskingKey != 0);
        }

        /// <summary>
        /// Creates a frame from a raw header byte, stating explicitly whether the MASK bit was set.
        /// </summary>
        internal static DataFrame CreateDataFrame(byte dataFrameFlag, byte[] data, UInt32 maskingKey, bool masked)
        {
            var dataArray = new ArraySegment<byte>(data);
            return new DataFrame(dataFrameFlag, dataArray, maskingKey, masked);
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
            var dataArray = new ArraySegment<byte>(data);
            return new DataFrame(FIN, false, false, false, opcode, maskingKey, dataArray);
        }

        /// <summary>
        /// Splits a stream into a sequence of data frames.
        /// Large payloads are fragmented across multiple continuation frames.
        /// </summary>
        /// <param name="opcode">OpCode for the first frame; subsequent frames use Continuation.</param>
        /// <param name="maskingKey">Optional 4-byte masking key.</param>
        /// <param name="data">The payload stream to read from.</param>
        /// <param name="dataFrameLength">Max payload per frame (default 65535).</param>
        public static IEnumerable<DataFrame> CreateDataFrame(OpCode opcode, Stream data, UInt32 maskingKey = 0, int dataFrameLength = ushort.MaxValue)
        {
            byte[] bytes = new byte[dataFrameLength];
            int readNums = 0;
            while (true)
            {
                int curReadNums = data.Read(bytes, readNums, dataFrameLength - readNums);
                // Read finished — emit final frame
                if (curReadNums == 0)
                {
                    yield return new DataFrame(true, false, false, false, opcode, maskingKey, new ArraySegment<byte>(bytes, 0, readNums));
                    yield break;
                }
                readNums += curReadNums;
                if (readNums == dataFrameLength)
                {
                    // Buffer full — emit non-final fragment
                    yield return new DataFrame(false, false, false, false, opcode, maskingKey, new ArraySegment<byte>(bytes));
                    opcode = OpCode.Continuation;
                    bytes = new byte[dataFrameLength];
                    readNums = 0;
                }
            }
        }

        /// <summary>True if this is the final fragment of a message.</summary>
        public bool FIN => _dataFrameFlag >> 7 == 1;

        /// <summary>Reserved bit 1.</summary>
        public bool RSV1 => (_dataFrameFlag & 0x40) == 0x40;

        /// <summary>Reserved bit 2.</summary>
        public bool RSV2 => (_dataFrameFlag & 0x20) == 0x20;

        /// <summary>Reserved bit 3.</summary>
        public bool RSV3 => (_dataFrameFlag & 0x10) == 0x10;

        /// <summary>Frame opcode (Text, Binary, Close, Ping, Pong, Continuation).</summary>
        public OpCode Opcode => (OpCode)(_dataFrameFlag & 0x0F);

        /// <summary>True if the payload is masked (the MASK bit is set).</summary>
        public bool Masked => _masked;

        /// <summary>The 4-byte masking key. Meaningful only when <see cref="Masked"/> is true.</summary>
        public UInt32 MaskingKey => _maskingKey;

        /// <summary>Raw first byte of the frame header.</summary>
        public byte DataFrameFlag => _dataFrameFlag;

        /// <summary>Get dataframe header length</summary>
        public int DataFrameHeaderLength
        {
            get
            {
                int length = 2;
                if (Masked)
                {
                    length += 4;
                }
                // 64-bit extended payload length (127)
                if (_data.Count > ushort.MaxValue)
                {
                    length += 8;
                }
                // 16-bit extended payload length (126)
                else if (_data.Count > 125)
                {
                    length += 2;
                }
                return length;
            }
        }
        /// <summary>
        /// Serializes the frame header (2–14 bytes) according to RFC 6455.
        /// Supports payload lengths up to <see cref="int.MaxValue"/> (64-bit extended length).
        /// </summary>
        public byte[] DataFrameHeader
        {
            get
            {
                byte[] header = new byte[DataFrameHeaderLength];
                DataFrameHeaderFull(ref header);
                return header;
            }
        }
        /// <summary>
        /// bytes array will be fulled dataframe header data.
        /// Every byte of the header is written, so the array does not need to be zeroed
        /// beforehand and may come from an <see cref="System.Buffers.ArrayPool{T}"/>.
        /// <paramref name="bytes">the bytes array which be fulled</paramref>
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void DataFrameHeaderFull(ref byte[] bytes)
        {
            if (DataFrameHeaderLength > bytes.Length)
            {
                throw new Exception($"array length must be large then DataFrameHeaderLength = {DataFrameHeaderLength}");
            }
            int curIndex = 0;
            // 64-bit extended payload length (127)
            if (_data.Count > ushort.MaxValue)
            {
                bytes[0] = _dataFrameFlag;
                bytes[1] = 127;
                // The payload length is an int, so the high 4 bytes of the 64-bit field are
                // always zero. They must still be written explicitly: the caller's buffer is
                // not guaranteed to be zeroed (ArrayPool.Rent does not clear it).
                bytes[2] = 0;
                bytes[3] = 0;
                bytes[4] = 0;
                bytes[5] = 0;
                bytes[6] = (byte)((_data.Count >> 24) & 0xFF);
                bytes[7] = (byte)((_data.Count >> 16) & 0xFF);
                bytes[8] = (byte)((_data.Count >> 8) & 0xFF);
                bytes[9] = (byte)((_data.Count) & 0xFF);
                curIndex = 9;
            }
            // 16-bit extended payload length (126)
            else if (_data.Count > 125)
            {
                bytes[0] = _dataFrameFlag;
                bytes[1] = 126;
                bytes[2] = (byte)((_data.Count >> 8) & 0xFF);
                bytes[3] = (byte)((_data.Count) & 0xFF);
                curIndex = 3;
            }
            // 7-bit payload length (≤125)
            else
            {
                bytes[0] = _dataFrameFlag;
                bytes[1] = (byte)Data.Count;
                curIndex = 1;
            }
            if (Masked)
            {
                bytes[1] |= 0x80;
                bytes[++curIndex] = (byte)_maskingKey;
                bytes[++curIndex] = (byte)(_maskingKey >> 8);
                bytes[++curIndex] = (byte)(_maskingKey >> 16);
                bytes[++curIndex] = (byte)(_maskingKey >> 24);
            }
        }

        /// <summary>The payload data.</summary>
        public ArraySegment<byte> Data => _data;
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
    }
}