using System.ComponentModel;
using System.Net;
using System.Runtime.InteropServices;
using LHZ.WebSocket.Server.Enums;

namespace LHZ.WebSocket.Server.Core;

/// <summary>
/// DataFrame class represents a WebSocket data frame and provides methods to read and parse the frame from a stream.
/// It handles the WebSocket frame header, payload length, masking key, and payload data according to the WebSocket protocol specification (RFC 6455).
/// </summary>
public class DataFrame
{
    private readonly Stream _stream;
    private ulong _payloadLength;
    private ulong _readLength;
    private byte[] _maskingKey;
    /// <summary>
    /// Initializes a new instance of the DataFrame class by reading and parsing a Web
    /// </summary>
    /// <param name="stream">The stream to read the data frame from.</param>
    public DataFrame(Stream stream)
    {
        _stream = stream;
        Init();
    }
    private byte[] StreamReadExact(int count)
    {
        byte[] buffer = new byte[count];
        int bytesReadTotal = 0;
        while (bytesReadTotal < count)
        {
            int bytesRead = _stream.Read(buffer, bytesReadTotal, count - bytesReadTotal);
            if (bytesRead == 0)
                throw new EndOfStreamException("Stream closed while reading data frame.");
            bytesReadTotal += bytesRead;
        }
        return buffer;
    }
    private uint BytesToUnin32(byte[] bytes)
    {
        return ((uint)bytes[0]<<24) 
        | ((uint)bytes[1]<<16) 
        | ((uint)bytes[2]<<8) 
        | (uint)bytes[3];
    }
    private ulong BytesToUnin64(byte[] bytes)
    {
        return ((ulong)bytes[0]<<56) 
        | ((ulong)bytes[1]<<48) 
        | ((ulong)bytes[2]<<40) 
        | ((ulong)bytes[3]<<32) 
        | ((ulong)bytes[4]<<24) 
        | ((ulong)bytes[5]<<16) 
        | ((ulong)bytes[6]<<8) 
        | (ulong)bytes[7];
    }
    private ushort BytesToUnin16(byte[] bytes)
    {
        return (ushort)(((ushort)bytes[0]<<8) | (ushort)bytes[1]);
    }
    private void Init()
    {
        var header = StreamReadExact(4);
        _dataFrameHeader = BytesToUnin32(header);
        byte payloadLengthBase = PayloadLengthBase;
        if (payloadLengthBase <= 125)
        {
            _payloadLength = payloadLengthBase;
            if (Masked)
            {
                _maskingKey = new byte[4];
                _maskingKey[0] = (byte)((_dataFrameHeader >> 8) & 0xFF);
                _maskingKey[1] = (byte)(_dataFrameHeader & 0xFF);
                var maskKey = StreamReadExact(2);
                _maskingKey[2] = maskKey[0];
                _maskingKey[3] = maskKey[1];

            }
            return;
        }
        if (payloadLengthBase == 126)
        {
            _payloadLength = _dataFrameHeader & 0x0000FFFFu;
        }
        else if(payloadLengthBase == 127)
        {
            var extendedPayloadLength = StreamReadExact(6);
            extendedPayloadLength = new byte[] { (byte)((_dataFrameHeader >> 8) & _dataFrameHeader & 0xFF), 0 }.Concat(extendedPayloadLength).ToArray();
            _payloadLength = BytesToUnin64(extendedPayloadLength);
        }
        if(Masked)
        {
            _maskingKey = StreamReadExact(4);
        }
    }
    public byte[] _dataFrame;
    private uint _dataFrameHeader;
    public bool FIN => _dataFrameHeader >> 31 == 1;
    public bool RSV1 => (_dataFrameHeader & 0x40000000u) == 0x40000000u;
    public bool RSV2 => (_dataFrameHeader & 0x20000000u) == 0x20000000u;
    public bool RSV3 => (_dataFrameHeader & 0x10000000u) == 0x10000000u;
    public Opcode Opcode => (Opcode)((_dataFrameHeader >> 24) & 0x0Fu);
    public bool Masked => (_dataFrameHeader & 0x00800000u) == 0x00800000u;
    private byte PayloadLengthBase => (byte)((_dataFrameHeader >> 16) & 0x7Fu);
    public ulong PayloadLength => _payloadLength;
    public byte[] MaskingKey =>_maskingKey;
    public int Read(byte[] buffer, int offset, int count)
    {
        count = _payloadLength - _readLength > (ulong)count ? count : (int)(_payloadLength - _readLength);
        if (count == 0)
        {
            return 0;
        }
        int readNums = _stream.Read(buffer, offset, count);
        if (!Masked)
        {
            return readNums;
        }
        for (int i = 0; i < readNums; i++)
        {
            buffer[offset + i] ^= _maskingKey[_readLength++ % 4];
        }
        return readNums;
    }
}
