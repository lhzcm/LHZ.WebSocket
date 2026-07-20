using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using LHZ.WebSocket.Server.Core;

namespace LHZ.WebSocket.Server;

public class WebSocketClient
{
    private readonly TcpClient _tcpClient;
    private readonly NetworkStream _networkStream;
    public event Action<WebSocketClient, byte[]>? OnMessageReceived;
    public WebSocketClient(TcpClient tcpClient)
    {
        _tcpClient = tcpClient;
        _networkStream = tcpClient.GetStream();
    }
    public void StartReceiving()
    {
        Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    using var stream = new WebSocketStream(_networkStream);

                    if(stream.CanReadLength)
                    {
                        byte[] buffer = new byte[stream.Length];
                        int totalBytesRead = 0;
                        
                        while (totalBytesRead < buffer.Length)
                        {
                            int bytesRead = stream.Read(buffer, 0, buffer.Length);
                            totalBytesRead += bytesRead;
                        }
                        OnMessageReceived?.Invoke(this, buffer);
                    }
                    else
                    {
                        byte[] buffer = new byte[4096]; // 4KB buffer
                        int bytesRead;
                        using var memoryStream = new MemoryStream();
                        while ((bytesRead = stream.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            memoryStream.Write(buffer, 0, bytesRead);
                        }
                        OnMessageReceived?.Invoke(this, memoryStream.ToArray());
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error receiving data: {ex.Message}");
            }
        });
    }
}
