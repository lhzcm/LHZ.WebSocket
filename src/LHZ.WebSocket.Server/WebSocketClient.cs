using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using LHZ.WebSocket.Server.Core;
using LHZ.WebSocket.Server.Enums;

namespace LHZ.WebSocket.Server;

public class WebSocketClient : IDisposable
{
    private readonly TcpClient _tcpClient;
    private readonly NetworkStream _networkStream;
    private readonly Channel<DataFrame> _channel = Channel.CreateBounded<DataFrame>(1024);
    private readonly CancellationTokenSource _cts = new CancellationTokenSource();
    public event EventHandler<WebSocketClient, string>? OnMessageReceived;
     public event EventHandler<WebSocketClient, byte[]>? OnBytesReceived;
    public event EventHandler<WebSocketClient, CloseMessage>? OnCloseRecived;

    public event Action<WebSocketClient>? OnClientClose;
    public ClientStatus _clientStatus = ClientStatus.Wait;
    public ClientStatus Status => _clientStatus;
    public WebSocketClient(TcpClient tcpClient)
    {
        _tcpClient = tcpClient;
        _networkStream = tcpClient.GetStream();
    }
    public void SendMessage(string message)
    {
        Send(OpCode.Text, System.Text.Encoding.UTF8.GetBytes(message));
    }
    public void Send(OpCode opcode, byte[] bytes)
    {
        var dataFrame = DataFrame.CreateDataFrame(opcode, true, null, bytes);
        _channel.Writer.WriteAsync(dataFrame).GetAwaiter().GetResult();
    }
    public void Open()
    {
        if(_clientStatus == ClientStatus.Wait)
        {
            _clientStatus = ClientStatus.Opend;
            StartReceiving();
            StartSending();
        }
    }
    public void Close()
    {
        if (_clientStatus == ClientStatus.Close)
        {
            return;
        }
        _cts.Cancel();
        _tcpClient.Dispose();
        _clientStatus = ClientStatus.Close;
        OnClientClose?.Invoke(this);
    }
    public void StartReceiving()
    {
        Task.Run(async () =>
        {
            try
            {
                var dataFrameReader = new DataFrameReader(_networkStream);
                List<DataFrame> dataFrames = new List<DataFrame>();
                await foreach(var item in dataFrameReader.ReadAsync(_cts.Token))
                {
                    if(_cts.Token.IsCancellationRequested)
                    {
                        break;
                    }
                    if(item.FIN)
                    {
                        if(dataFrames.Count == 0)
                        {
                            MessageProcessing(item.Opcode, item.Data.ToArray());
                            continue;
                        }
                        dataFrames.Add(item);

                        int count = dataFrames.Sum(n=>n.Data.Count);
                        var bytes = new byte[count];
                        int offset = 0;
                        foreach(var dataFrame in dataFrames)
                        {
                            Array.Copy(dataFrame.Data.ToArray(), 0, bytes, offset, dataFrame.Data.Count);
                            offset += dataFrame.Data.Count;
                        }
                        MessageProcessing(dataFrames[0].Opcode, bytes);
                        dataFrames.Clear();
                    }
                    dataFrames.Add(item);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error receiving data: {ex.Message}");
            }
        });
    }
    protected virtual void MessageProcessing(OpCode opCode, byte[] data)
    {
        switch (opCode)
        {
            case OpCode.Binary: OnBytesReceived?.Invoke(this, data); break;
            case OpCode.Close:
                {
                    if (data.Length < 2)
                        throw new Exception("Close Frame has Error");
                    CloseCode closeCode = (CloseCode)((data[0] << 8) | data[1]);
                    if (!Enum.IsDefined<CloseCode>(closeCode))
                        throw new Exception("CloseCode has not define");
                    var closeMessage = new CloseMessage(closeCode, Encoding.UTF8.GetString(data, 2, data.Length - 2));
                    OnCloseRecived?.Invoke(this, closeMessage);
                    break;
                }
            case OpCode.Text:
                {
                    var str = System.Text.Encoding.UTF8.GetString(data);
                    OnMessageReceived?.Invoke(this, str); break;
                }
            case OpCode.Ping:
                {
                    //TODO
                    throw new Exception("Not Support");
                }
            case OpCode.Pong:
                {
                    //TODO
                    throw new Exception("Not Support");
                }
            default: throw new Exception("OpCode not Support");
        }
    }
    public void StartSending()
    {
        Task.Run(async () =>
        {
            try
            {
                while (!_cts.Token.IsCancellationRequested)
                {
                    var dataFrame = await _channel.Reader.ReadAsync(_cts.Token);
                    await _networkStream.WriteAsync(dataFrame.DataFrameHeader, _cts.Token);
                    await _networkStream.WriteAsync(dataFrame.Data, _cts.Token);
                    await _networkStream.FlushAsync(_cts.Token);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error Sending data: {ex.Message}");
            }
        });
    }
    public void Dispose()
    {
        Close();
    }
}
