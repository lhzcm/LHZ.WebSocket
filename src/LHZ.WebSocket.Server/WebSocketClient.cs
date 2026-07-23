using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Channels;
using LHZ.WebSocket.Server.Core;
using LHZ.WebSocket.Server.Enums;

namespace LHZ.WebSocket.Server;

/// <summary>
/// Represents a single WebSocket client connection.
/// Manages frame-level send/receive with separate reader and writer background tasks.
/// </summary>
public class WebSocketClient : IDisposable
{
    private readonly TcpClient _tcpClient;
    private readonly NetworkStream _networkStream;

    /// <summary>Bounded channel for outgoing data frames (producer-consumer).</summary>
    private readonly Channel<DataFrame> _channel;

    private readonly CancellationTokenSource _cts = new CancellationTokenSource();

    /// <summary>Raised when a complete text message is received.</summary>
    public event EventHandler<WebSocketClient, string>? OnMessageReceived;

    /// <summary>Raised when a complete binary message is received.</summary>
    public event EventHandler<WebSocketClient, byte[]>? OnBytesReceived;

    /// <summary>Raised when a close frame is received from the peer.</summary>
    public event EventHandler<WebSocketClient, CloseMessage>? OnCloseRecived;

    /// <summary>Raised when this client disconnects (local or remote).</summary>
    public event Action<WebSocketClient>? OnClientClose;

    public ClientStatus _clientStatus = ClientStatus.Connection;

    /// <summary>Current connection status.</summary>
    public ClientStatus Status => _clientStatus;

    public WebSocketClient(TcpClient tcpClient, int capacity)
    {
        _tcpClient = tcpClient;
        _networkStream = tcpClient.GetStream();
        _channel = Channel.CreateBounded<DataFrame>(capacity);
    }

    public WebSocketClient(TcpClient tcpClient)
    {
        _tcpClient = tcpClient;
        _networkStream = tcpClient.GetStream();
        _channel = Channel.CreateBounded<DataFrame>(1024);
    }

    /// <summary>Sends a UTF-8 text message to the peer.</summary>
    public void SendMessage(string message)
    {
        Send(OpCode.Text, System.Text.Encoding.UTF8.GetBytes(message));
    }

    /// <summary>Sends raw binary data to the peer.</summary>
    public void SendByte(byte[] bytes)
    {
        Send(OpCode.Binary, bytes);
    }

    /// <summary>Enqueues a data frame onto the outgoing channel.</summary>
    private void Send(OpCode opCode, byte[] bytes)
    {
        var dataFrame = DataFrame.CreateDataFrame(opCode, true, null, bytes);
        _channel.Writer.WriteAsync(dataFrame).GetAwaiter().GetResult();
    }

    /// <summary>Starts the reader and sender background tasks.</summary>
    public void Open()
    {
        if (_clientStatus == ClientStatus.Connection)
        {
            _clientStatus = ClientStatus.Opend;
            StartReceiver();
            StartSender();
        }
    }

    /// <summary>Cancels background tasks and disposes the underlying TCP connection.</summary>
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

    /// <summary>
    /// Background task that continuously reads WebSocket frames from the network stream,
    /// reassembles fragmented messages, and dispatches them via <see cref=\"ReceiveProcessing\"/>.
    /// </summary>
    private void StartReceiver()
    {
        Task.Run(async () =>
        {
            try
            {
                var dataFrameReader = new DataFrameReader(_networkStream);
                List<DataFrame> dataFrames = new List<DataFrame>();
                await foreach (var item in dataFrameReader.ReadAsync(_cts.Token))
                {
                    if (_cts.Token.IsCancellationRequested)
                    {
                        break;
                    }
                    // FIN frame received — either a single-frame message or end of a fragmented message
                    if (item.FIN)
                    {
                        if (dataFrames.Count == 0)
                        {
                            ReceiveProcessing(item.Opcode, item.RSV1, item.RSV2, item.RSV3, item.Data.ToArray());
                            continue;
                        }
                        dataFrames.Add(item);

                        // Concatenate all continuation frames into one payload
                        int count = dataFrames.Sum(n => n.Data.Count);
                        var bytes = new byte[count];
                        int offset = 0;
                        foreach (var dataFrame in dataFrames)
                        {
                            Array.Copy(dataFrame.Data.ToArray(), 0, bytes, offset, dataFrame.Data.Count);
                            offset += dataFrame.Data.Count;
                        }
                        ReceiveProcessing(dataFrames[0].Opcode, dataFrames[0].RSV1, dataFrames[0].RSV2, dataFrames[0].RSV3, bytes);
                        dataFrames.Clear();
                    }
                    dataFrames.Add(item);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error receiving data: {ex.Message}");
                Close();
            }
        });
    }

    /// <summary>
    /// Dispatches a fully reassembled message to the appropriate event handler based on opcode.
    /// Override to customize ping/pong handling.
    /// </summary>
    protected virtual void ReceiveProcessing(OpCode opCode, bool RSV1, bool RSV2, bool RSV3, byte[] data)
    {
        switch (opCode)
        {
            case OpCode.Binary: OnBytesReceived?.Invoke(this, data); break;
            case OpCode.Close:
                {
                    if (data.Length < 2)
                        throw new Exception("Close Frame has Error");
                    // First two bytes = close status code (big-endian)
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
                    PingProcessing(data);
                    break;
                }
            case OpCode.Pong:
                {
                    PongProcessing(data);
                    break;
                }
            default: throw new Exception("OpCode not Support");
        }
    }

    /// <summary>Called when a ping frame is received. Override to add custom behavior.</summary>
    protected virtual void PingProcessing(byte[] data)
    {
    }

    /// <summary>Called when a pong frame is received. Default sends an empty pong back.</summary>
    protected virtual void PongProcessing(byte[] data)
    {
        Send(OpCode.Pong, Array.Empty<byte>());
    }

    /// <summary>
    /// Background task that dequeues outgoing data frames from the channel
    /// and writes them to the network stream.
    /// </summary>
    private void StartSender()
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
                Close();
            }
        });
    }

    public void Dispose()
    {
        Close();
    }
}
