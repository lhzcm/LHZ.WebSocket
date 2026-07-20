using System.Net.Sockets;
using LHZ.WebSocket.Server.Enums;

namespace LHZ.WebSocket.Server.Core;

public sealed class WebSocketStream : Stream
{
    private readonly DataFrame _firstDataFrame;
    private DataFrame _curDataFrame;
    private readonly NetworkStream _networkStream;
    public event Action OnStreamClosed;
    public override bool CanRead => true;

    public override bool CanSeek => false;

    public override bool CanWrite => false;

    public override long Length => _firstDataFrame.FIN ? (long)_firstDataFrame.PayloadLength : throw new NotSupportedException();
    public bool CanReadLength => _firstDataFrame.FIN ? true : false;
    public Opcode Opcode => _firstDataFrame.Opcode;

    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
    internal WebSocketStream(NetworkStream networkStream)
    {
        _networkStream = networkStream;
        _firstDataFrame = new DataFrame(networkStream);
        _curDataFrame = _firstDataFrame;
    }

    public override void Flush()
    {
        throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        int readNums = _curDataFrame.Read(buffer, offset, count);
        if (readNums == 0 && !_curDataFrame.FIN)
        {
            _curDataFrame = new DataFrame(_networkStream);
            readNums = _curDataFrame.Read(buffer, offset, count);
        }
        if(readNums == 0 && _curDataFrame.FIN)
        {
            OnStreamClosed?.Invoke();
        }
        return readNums;
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }
    private byte[] ReadBinaryMessage()
    {
        List<byte[]> bytesList = new List<byte[]>();
        byte[] buffer = new byte[_firstDataFrame.PayloadLength];
        int length = 0, offset = 0;
        while (true)
        {
            int readNums = Read(buffer, offset, buffer.Length);
            if (readNums == 0)
            {
                break;
            }
            offset += readNums;
            length += readNums;
            if(offset == buffer.Length)
            {
                bytesList.Add(buffer);
                buffer = new byte[_firstDataFrame.PayloadLength];
                offset = 0;
            }
        }
        byte[] result = new byte[length];
        int resultOffset = 0;
        foreach (var bytes in bytesList)
        {
            Array.Copy(bytes, 0, result, resultOffset, bytes.Length);
            resultOffset += bytes.Length;
        }
        return result;
    }
    public override void Close()
    {
        base.Close();
    }
}
