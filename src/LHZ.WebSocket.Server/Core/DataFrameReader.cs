namespace LHZ.WebSocket.Server.Core;

public class DataFrameReader
{
    private readonly Stream _stream;
    public DataFrameReader(Stream stream)
    {
        _stream = stream;
    }
    public IEnumerable<DataFrame> Read()
    {
        while(true)
        {
            var frame =  ReadFrame();
            yield return frame;
            if(frame.Opcode == Enums.OpCode.Close)
            {
                yield break;
            }
        }
    }
    public async IAsyncEnumerable<DataFrame> ReadAsync(CancellationToken cancellationToken = default)
    {
        while(!cancellationToken.IsCancellationRequested)
        {
            var frame = await ReadFrameAsync(cancellationToken);
            yield return frame;
        }
    }
    private DataFrame ReadFrame()
    {
        var header = StreamReadExact(2);
        int payloadLength = (byte)(header[1] & 0x7F);
        bool masked = (header[1] & 0x80) == 0x80;
        byte[]? maskingKey = null;
        if (payloadLength == 126)
        {
            var lengthBytes = StreamReadExact(2);
            payloadLength = (((int)lengthBytes[0]) << 8) | lengthBytes[1];
        }
        else if(payloadLength == 127)
        {
            var lengthBytes = StreamReadExact(8);
            if(lengthBytes[0] != 0 || lengthBytes[1] != 0 || lengthBytes[2] != 0 || lengthBytes[3] != 0)
            {
                throw new Exception($"Single frame data not larger than {int.MaxValue} bytes");
            }
            payloadLength = (((int)lengthBytes[4]) << 24) | (((int)lengthBytes[5]) << 16) | (((int)lengthBytes[6]) << 8) | lengthBytes[7];
        }
        if(masked)
        {
            maskingKey = StreamReadExact(4);
        }
        return DataFrame.CreateDataFrame(header[0], maskingKey, StreamReadExact(payloadLength));
    }
    private async Task<DataFrame> ReadFrameAsync(CancellationToken cancellationToken = default)
    {
        var header = await StreamReadExactAsync(2, cancellationToken);
        int payloadLength = (byte)(header[1] & 0x7F);
        bool masked = (header[1] & 0x80) == 0x80;
        byte[]? maskingKey = null;
        if (payloadLength == 126)
        {
            var lengthBytes = await StreamReadExactAsync(2, cancellationToken);
            payloadLength = (((int)lengthBytes[0]) << 8) | lengthBytes[1];
        }
        else if(payloadLength == 127)
        {
            var lengthBytes = await StreamReadExactAsync(8, cancellationToken);
            if(lengthBytes[0] != 0 || lengthBytes[1] != 0 || lengthBytes[2] != 0 || lengthBytes[3] != 0)
            {
                throw new Exception($"Single frame data not larger than {int.MaxValue} bytes");
            }
            payloadLength = (((int)lengthBytes[4]) << 24) | (((int)lengthBytes[5]) << 16) | (((int)lengthBytes[6]) << 8) | lengthBytes[7];
        }
        if(masked)
        {
            maskingKey = await StreamReadExactAsync(4, cancellationToken);
        }
        return DataFrame.CreateDataFrame(header[0], maskingKey, await StreamReadExactAsync(payloadLength, cancellationToken));
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
    private async Task<byte[]> StreamReadExactAsync(int count, CancellationToken cancellationToken = default)
    {
        byte[] buffer = new byte[count];
        int bytesReadTotal = 0;
        while (bytesReadTotal < count && !cancellationToken.IsCancellationRequested)
        {
            int bytesRead = await _stream.ReadAsync(buffer, bytesReadTotal, count - bytesReadTotal, cancellationToken);
            if (bytesRead == 0)
                throw new EndOfStreamException("Stream closed while reading data frame.");
            bytesReadTotal += bytesRead;
        }
        return buffer;
    }
}
