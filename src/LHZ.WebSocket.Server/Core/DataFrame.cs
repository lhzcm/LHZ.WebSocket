using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using LHZ.WebSocket.Server.Enums;

namespace LHZ.WebSocket.Server.Core;

public class DataFrame
{          
    // 0 1 2 3 4 5 6 7 
    //+-+-+-+-+-------+
    //|F|R|R|R| opcode|
    //|I|S|S|S|  (4)  |
    //|N|V|V|V|       |
    //| |1|2|3|       |
    //+-+-+-+-+-------+
    private byte _dataDataFrameFlag;
    private byte[]? _maskingKey;
    private ArraySegment<byte> _data;
    private DataFrame(bool FIN, bool RSV1, bool RSV2, bool RSV3, OpCode opcode, byte[]? maskingKey, ArraySegment<byte> data)
    {
        _data = data;
        if(FIN)
        {
            _dataDataFrameFlag |= 0x80;
        }
        if(RSV1)
        {
            _dataDataFrameFlag |= 0x40;
        }
        if(RSV2)
        {
            _dataDataFrameFlag |= 0x20;
        }
        if(RSV3)
        {
            _dataDataFrameFlag |= 0x10;
        }
        _dataDataFrameFlag |= (byte)opcode;
        if(maskingKey != null && maskingKey.Length != 4)
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
    private DataFrame(byte dataDataFrameFlag, byte[]? maskingKey, ArraySegment<byte> data)
    {
        _data = data;
        _dataDataFrameFlag = dataDataFrameFlag;
        if(maskingKey != null && maskingKey.Length != 4)
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
    internal static DataFrame CreateDataFrame(byte dataDataFrameFlag, byte[]? maskingKey, byte[] data)
    {
        var dataArray = new ArraySegment<byte>(data);
        return new DataFrame(dataDataFrameFlag, maskingKey, dataArray);
    }
    public static DataFrame CreateDataFrame(OpCode opcode, bool FIN, byte[]? maskingKey, byte[] data)
    {
        var dataArray = new ArraySegment<byte>(data);
        return new DataFrame(FIN, false, false, false, opcode, maskingKey, dataArray);
    }
    /// <summary>
    /// Create DataDataFrame by Stream
    /// </summary>
    /// <param name="opcode">OpCode</param>
    /// <param name="MaskingKey">MaskingKey</param>
    /// <param name="data">DataStream</param>
    /// <returns></returns>
    public static IEnumerable<DataFrame> CreateDataFrame(OpCode opcode, byte[]? maskingKey, Stream data, int dataDataFrameLength = ushort.MaxValue)
    {
        byte[] bytes = new byte[dataDataFrameLength];
        int readNums = 0;
        while(true)
        {
            int curReadNums = data.Read(bytes, readNums, dataDataFrameLength - readNums);
            //Read Finish
            if(curReadNums == 0)     
            {
                yield return new DataFrame(true, false, false, false, opcode, maskingKey, new ArraySegment<byte>(bytes, 0, readNums));
                yield break;
            }
            readNums += curReadNums;
            if(readNums == dataDataFrameLength)
            {
                yield return new DataFrame(false, false, false, false, opcode, maskingKey, new ArraySegment<byte>(bytes));
                opcode = OpCode.Continuation;
                bytes = new byte[dataDataFrameLength];
                readNums = 0;
            }
        }
    }
    public bool FIN => _dataDataFrameFlag >> 7 == 1;
    public bool RSV1 => (_dataDataFrameFlag & 0x40) == 0x40;
    public bool RSV2 => (_dataDataFrameFlag & 0x20) == 0x20;
    public bool RSV3 => (_dataDataFrameFlag & 0x10) == 0x10;
    public OpCode Opcode => (OpCode)(_dataDataFrameFlag & 0x0F);
    public bool Masked => _maskingKey != null;
    public byte[]? MaskingKey => _maskingKey;
    public byte DataDataFrameFlag => _dataDataFrameFlag;
    public byte[] DataFrameHeader
    {
        get
        {
            int length = 2;
            if(Masked)
            {
                length += 4;
            }
            byte[] header;
            if(_data.Count > ushort.MaxValue)
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
            if(_data.Count > 125)
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
            header = new byte[length];
            header[0] = _dataDataFrameFlag;
            header[1] = (byte)Data.Count;
            if(_maskingKey != null)
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
    public ArraySegment<byte> Data => _data;
}
