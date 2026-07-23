using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using LHZ.WebSocket.Server.Enums;

namespace LHZ.WebSocket.Server.Core;

/// <summary>
/// Represents a WebSocket data frame as defined in RFC 6455.
/// Handles masking/unmasking and header serialization.
/// </summary>
public class DataFrame
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
    private byte _dataDataFrameFlag;

    /// <summary>4-byte masking key (null if not masked).</summary>
    private byte[]? _maskingKey;

    /// <summary>Payload data segment.</summary>
    private ArraySegment<byte> _data;

    /// <summary>
    /// Creates a new outgoing frame. Applies XOR masking if a key is provided.
    /// </summary>
    private DataFrame(bool FIN, bool RSV1, bool RSV2, bool RSV3, OpCode opcode, byte[]? maskingKey, ArraySegment<byte> data)
    {
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
        if (maskingKey != null && maskingKey.Length != 4)
        {
            throw new ArgumentException(nameof(maskingKey) + " is error data");
        }
        _maskingKey = maskingKey;
        if (_maskingKey != null)
        {
            int offset = data.Offset;
            var array = data.Array ?? Array.Empty<byte>();
            for (int i = offset; i < data.Offset + data.Count; i++)
            {
                array[i] ^= _maskingKey[(i - offset) % 4];
            }
        }
    }

    /// <summary>
    /// Creates a frame from a pre-parsed header byte (used when reading incoming frames).
    /// </summary>
    private DataFrame(byte dataDataFrameFlag, byte[]? maskingKey, ArraySegment<byte> data)
    {
        _data = data;
        _dataDataFrameFlag = dataDataFrameFlag;
        if (maskingKey != null && maskingKey.Length != 4)
        {
            throw new ArgumentException(nameof(maskingKey) + " is error data");
        }
        _maskingKey = maskingKey;
        if (_maskingKey != null)
        {
            int offset = data.Offset;
            var array = data.Array ?? Array.Empty<byte>();
            for (int i = offset; i < data.Offset + data.Count; i++)
            {
                array[i] ^= _maskingKey[(i - offset) % 4];
            }
        }
    }

    /// <summary>Creates a frame from a raw header byte (used by DataFrameReader).</summary>
    internal static DataFrame CreateDataFrame(byte dataDataFrameFlag, byte[]? maskingKey, byte[] data)
    {
        var dataArray = new ArraySegment<byte>(data);
        return new DataFrame(dataDataFrameFlag, maskingKey, dataArray);
    }

    /// <summary>Creates an outgoing frame with the given opcode and payload.</summary>
    public static DataFrame CreateDataFrame(OpCode opcode, bool FIN, byte[]? maskingKey, byte[] data)
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
    public static IEnumerable<DataFrame> CreateDataFrame(OpCode opcode, byte[]? maskingKey, Stream data, int dataDataFrameLength = ushort.MaxValue)
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
    public bool Masked => _maskingKey != null;

    /// <summary>The 4-byte masking key, or null.</summary>
    public byte[]? MaskingKey => _maskingKey;

    /// <summary>Raw first byte of the frame header.</summary>
    public byte DataDataFrameFlag => _dataDataFrameFlag;

    /// <summary>
    /// Serializes the frame header (2–14 bytes) according to RFC 6455.
    /// Supports payload lengths up to 2^63-1 (127-bit extended length).
    /// </summary>
    public byte[] DataFrameHeader
    {
        get
        {
            int length = 2;
            if (Masked)
            {
                length += 4;
            }
            byte[] header;

            // 64-bit extended payload length (127)
            if (_data.Count > ushort.MaxValue)
            {
                length += 8;
                header = new byte[length];
                header[0] = _dataDataFrameFlag;
                header[1] = 127;
                header[6] = (byte)((_data.Count >> 24) & 0xFF);
                header[7] = (byte)((_data.Count >> 16) & 0xFF);
                header[8] = (byte)((_data.Count >> 8) & 0xFF);
                header[9] = (byte)((_data.Count) & 0xFF);
                if (_maskingKey != null)
                {
                    header[1] |= 0x80;
                    header[10] = _maskingKey[0];
                    header[11] = _maskingKey[1];
                    header[12] = _maskingKey[2];
                    header[13] = _maskingKey[3];
                }
                return header;
            }

            // 16-bit extended payload length (126)
            if (_data.Count > 125)
            {
                length += 2;
                header = new byte[length];
                header[0] = _dataDataFrameFlag;
                header[1] = 126;
                header[2] = (byte)((_data.Count >> 8) & 0xFF);
                header[3] = (byte)((_data.Count) & 0xFF);
                if (_maskingKey != null)
                {
                    header[1] |= 0x80;
                    header[4] = _maskingKey[0];
                    header[5] = _maskingKey[1];
                    header[6] = _maskingKey[2];
                    header[7] = _maskingKey[3];
                }
                return header;
            }

            // 7-bit payload length (≤125)
            header = new byte[length];
            header[0] = _dataDataFrameFlag;
            header[1] = (byte)Data.Count;
            if (_maskingKey != null)
            {
                header[1] |= 0x80;
                header[2] = _maskingKey[0];
                header[3] = _maskingKey[1];
                header[4] = _maskingKey[2];
                header[5] = _maskingKey[3];
            }
            return header;
        }
    }

    /// <summary>The payload data.</summary>
    public ArraySegment<byte> Data => _data;
}
