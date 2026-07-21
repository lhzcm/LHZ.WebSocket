using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using LHZ.WebSocket.Server.Http;

namespace LHZ.WebSocket.Server;

public class WebSocketServer
{
    IPAddress _ip;
    int _port;
    public event Func<HttpContext, bool>? OnUpgradeRequest;
    public event Action<WebSocketClient>? OnClientConnected;
    private static readonly System.Collections.Concurrent.ConcurrentBag<WebSocketClient> _webSocketClients = new System.Collections.Concurrent.ConcurrentBag<WebSocketClient>();
    public WebSocketServer(IPAddress ip, int port)
    {
        _ip = ip;
        _port = port;
    }
    public WebSocketServer(int port)
    {
        _ip = IPAddress.Any;
        _port = port;
    }

    /// <summary>
    /// Starts the WebSocket server and listens for incoming connections.
    /// </summary>
    public void Start()
    {
        TcpListener listener = new TcpListener(_ip, _port);
        listener.Start();
        while (true)
        {
            TcpClient tcpClient = listener.AcceptTcpClient();
            Task.Run(() =>
            {
                HttpUpgradeHandler(tcpClient);
            });
        }
    }

    private void HttpUpgradeHandler(TcpClient tcpClient)
    {
        NetworkStream networkStream = tcpClient.GetStream();

        // Parse the HTTP upgrade request from the stream
        var httpRequest = new HttpRequest(networkStream);
        HttpContext httpContent = new HttpContext(httpRequest);
        if (OnUpgradeRequest?.Invoke(httpContent) == true)
        {
            var responseHeaders = new Dictionary<string, string>();
            responseHeaders.Add("Upgrade", "websocket");
            responseHeaders.Add("Connection", "Upgrade");

            string secWebSocketKey = httpContent.Request.Headers.GetValues("Sec-WebSocket-Key").First()
                ?? throw new InvalidOperationException("Missing Sec-WebSocket-Key header.");

            var sha1 = Convert.ToBase64String(
                SHA1.HashData(
                    System.Text.Encoding.UTF8.GetBytes(
                        secWebSocketKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")));

            responseHeaders.Add("Sec-WebSocket-Accept", sha1);

            networkStream.Write(System.Text.Encoding.UTF8.GetBytes("HTTP/1.1 101 Switching Protocols\n"));
            var headersStrBuild = new StringBuilder();
            foreach(var item in responseHeaders)
            {
                headersStrBuild.Append(item.Key);
                headersStrBuild.Append(":");
                headersStrBuild.Append(item.Value);
                headersStrBuild.Append("\n");
            }
            headersStrBuild.Append("\n");
            networkStream.Write(System.Text.Encoding.UTF8.GetBytes(headersStrBuild.ToString()));
            networkStream.Flush();
            var ret = new WebSocketClient(tcpClient);
            _webSocketClients.Add(ret);
            OnClientConnected?.Invoke(ret);
            ret.Open();
        }
        else
        {
            tcpClient.Close();
        }
    }
}
