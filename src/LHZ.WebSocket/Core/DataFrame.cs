using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection.Metadata.Ecma335;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
        private readonly byte _dataDataFrameFlag;

        /// <summary>
        /// use uint32 as 4-byte masking key (0 is not masked).
        /// map: [0x01, 0x02, 0x03, 0x04] -> 0x04030201
        /// </summary>
        private readonly UInt32 _maskingKey;

        /// <summary>Payload data segment.</summary>
        private ArraySegment<byte> _data;

        /// <summary>
        /// Creates a new outgoing frame. Applies XOR masking if a key is provided.
        /// The caller's buffer is never modified: masking is applied to a copy.
        /// </summary>
        private DataFrame(bool FIN, bool RSV1, bool RSV2, bool RSV3, OpCode opcode, UInt32 maskingKey, ArraySegment<byte> data)
        {
            _maskingKey = maskingKey;
            _data = data;
            if (FIN)
            {
                _dataDataFrameFlag |= 0x80;
            }
            if (RSV1)
            {
                _dataDataFrameFlag |= 0x40;
            }
            if (RSV2)
            {
                _dataDataFrameFlag |= 0x20;
            }
            if (RSV3)
            {
                _dataDataFrameFlag |= 0x10;
            }
            _dataDataFrameFlag |= (byte)opcode;
            ApplyMask(_data, _maskingKey);
        }

        /// <summary>
        /// Creates a frame from a pre-parsed header byte (used when reading incoming frames).
        /// The caller's buffer is never modified: unmasking is applied to a copy.
        /// </summary>
        private DataFrame(byte dataDataFrameFlag, ArraySegment<byte> data, UInt32 maskingKey = 0)
        {
            _maskingKey = maskingKey;
            _data = data;
            _dataDataFrameFlag = dataDataFrameFlag;
            ApplyMask(_data, _maskingKey);
        }

        /// <summary>
        /// XOR-masks (or unmask) the payload against the 4-byte key.
        /// Works on a copy so the original buffer passed by the caller is left untouched.
        /// </summary>
        private static void ApplyMask(ArraySegment<byte> data, UInt32 maskingKey)
        {
            var array = data.Array;
            if(array == null || maskingKey == 0)
            {
                return;
            }
            // Apply reverse byte rotation first
            var maskingKeyTemp = (maskingKey << (data.Offset % 4 * 8)) | (maskingKey >> (32 - data.Offset % 4 * 8));
            for (int i = data.Offset; i < data.Count; i++)
            {
                array[i] ^= (byte)(maskingKeyTemp >> ((i % 4) << 3));
            }
        }

        /// <summary>Creates a frame from a raw header byte (used by DataFrameReader).</summary>
        internal static DataFrame CreateDataFrame(byte dataDataFrameFlag, byte[] data, UInt32 maskingKey = 0)
        {
            var dataArray = new ArraySegment<byte>(data);
            return new DataFrame(dataDataFrameFlag, dataArray, maskingKey);
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
        /// <param name="dataDataFrameLength">Max payload per frame (default 65535).</param>
        public static IEnumerable<DataFrame> CreateDataFrame(OpCode opcode, Stream data, UInt32 maskingKey = 0, int dataDataFrameLength = ushort.MaxValue)
        {
            byte[] bytes = new byte[dataDataFrameLength];
            int readNums = 0;
            while (true)
            {
                int curReadNums = data.Read(bytes, readNums, dataDataFrameLength - readNums);
                // Read finished — emit final frame
                if (curReadNums == 0)
                {
                    yield return new DataFrame(true, false, false, false, opcode, maskingKey, new ArraySegment<byte>(bytes, 0, readNums));
                    yield break;
                }
                readNums += curReadNums;
                if (readNums == dataDataFrameLength)
                {
                    // Buffer full — emit non-final fragment
                    yield return new DataFrame(false, false, false, false, opcode, maskingKey, new ArraySegment<byte>(bytes));
                    opcode = OpCode.Continuation;
                    bytes = new byte[dataDataFrameLength];
                    readNums = 0;
                }
            }
        }

        /// <summary>True if this is the final fragment of a message.</summary>
        public bool FIN => _dataDataFrameFlag >> 7 == 1;

        /// <summary>Reserved bit 1.</summary>
        public bool RSV1 => (_dataDataFrameFlag & 0x40) == 0x40;

        /// <summary>Reserved bit 2.</summary>
        public bool RSV2 => (_dataDataFrameFlag & 0x20) == 0x20;

        /// <summary>Reserved bit 3.</summary>
        public bool RSV3 => (_dataDataFrameFlag & 0x10) == 0x10;

        /// <summary>Frame opcode (Text, Binary, Close, Ping, Pong, Continuation).</summary>
        public OpCode Opcode => (OpCode)(_dataDataFrameFlag & 0x0F);

        /// <summary>True if the payload is masked.</summary>
        public bool Masked => _maskingKey > 0;

        /// <summary>The 4-byte masking key, or null.</summary>
        public UInt32 MaskingKey => _maskingKey;

        /// <summary>Raw first byte of the frame header.</summary>
        public byte DataDataFrameFlag => _dataDataFrameFlag;

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
        /// Supports payload lengths up to 2^63-1 (127-bit extended length).
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
        /// bytes array will be fulled dataframe header data
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
                bytes[0] = _dataDataFrameFlag;
                bytes[1] = 127;
                bytes[6] = (byte)((_data.Count >> 24) & 0xFF);
                bytes[7] = (byte)((_data.Count >> 16) & 0xFF);
                bytes[8] = (byte)((_data.Count >> 8) & 0xFF);
                bytes[9] = (byte)((_data.Count) & 0xFF);
                curIndex = 9;
            }
            // 16-bit extended payload length (126)
            else if (_data.Count > 125)
            {
                bytes[0] = _dataDataFrameFlag;
                bytes[1] = 126;
                bytes[2] = (byte)((_data.Count >> 8) & 0xFF);
                bytes[3] = (byte)((_data.Count) & 0xFF);
                curIndex = 3;
            }
            // 7-bit payload length (≤125)
            else
            {
                bytes[0] = _dataDataFrameFlag;
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