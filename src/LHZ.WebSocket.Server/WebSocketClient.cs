using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using LHZ.WebSocket.Server.Core;
using LHZ.WebSocket.Server.Enums;

namespace LHZ.WebSocket.Server;

public sealed class WebSocketClient : IDisposable
{
    private readonly TcpClient _tcpClient;
    private readonly NetworkStream _networkStream;
    private readonly Channel<DataFrame> _channel = Channel.CreateBounded<DataFrame>(512);
    public event Action<WebSocketClient, byte[]>? OnMessageReceived;
    public event Action<WebSocketClient>? OnClientClose;
    public bool _isOpen = false;
    public bool IsOpen => _isOpen;
    public WebSocketClient(TcpClient tcpClient)
    {
        _tcpClient = tcpClient;
        _networkStream = tcpClient.GetStream();
    }
    public ValueTask SendMessageAsync(string message)
    {
        return SendAsync(Opcode.Text, System.Text.Encoding.UTF8.GetBytes(message));
    }
    public ValueTask SendAsync(Opcode opcode, byte[] bytes)
    {
        var dataFrame = DataFrame.CreateDataFrame(opcode, true, null, bytes);
        return _channel.Writer.WriteAsync(dataFrame);
    }
    public WebSocketClient Open()
    {
        if(!_isOpen)
        {
            _isOpen = true;
            StartReceiving();
            StartSending();
        }
        return this;
    }
    public void StartReceiving()
    {
        Task.Run(async () =>
        {
            try
            {
                var dataFrameReader = new DataFrameReader(_networkStream);
                List<byte[]> bytes = new List<byte[]>();
                foreach(var item in dataFrameReader.Read())
                {
                    var ret = item.Data.ToArray();
                    bytes.Add(ret);
                    if(item.FIN)
                    {
                        if(bytes.Count == 1)
                        {
                            OnMessageReceived?.Invoke(this, ret);
                            bytes.Clear();
                            continue;
                        }
                        int count = bytes.Sum(n=>n.Length);
                        ret = new byte[count];
                        int offset = 0;
                        foreach(var array in bytes)
                        {
                            Array.Copy(array, 0, ret, offset, array.Length);
                            offset += array.Length;
                        }
                        OnMessageReceived?.Invoke(this, ret);
                        bytes.Clear();
                    }
                }
            }
            catch (Exception ex)
            {
                _tcpClient.Close();
                Console.WriteLine($"Error receiving data: {ex.Message}");
            }
        });
    }
    public void StartSending()
    {
        Task.Run(async () =>
        {
            try
            {
                while(true)
                {
                    var dataFrame = await _channel.Reader.ReadAsync();
                    await _networkStream.WriteAsync(dataFrame.DataFrameHeader);
                    await _networkStream.WriteAsync(dataFrame.Data);
                    await _networkStream.FlushAsync();
                }
            }
            catch (Exception ex)
            {
                _tcpClient.Close();
                Console.WriteLine($"Error Sending data: {ex.Message}");
            }
        });
    }

    public void Dispose()
    {
        _tcpClient.Client.Close();
        _tcpClient.Dispose();
    }
}
