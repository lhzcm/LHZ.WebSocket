using System.Net;
using System.Net.Sockets;
using System.Text;
using LHZ.WebSocket.Interfaces;

namespace LHZ.WebSocket.Test;

/// <summary>
/// WebSocketClient 基础功能测试。
/// 本测试类通过服务器端 OnUpgradeRequest 获取 IWebSocketClient 进行测试。
/// </summary>
public class WebSocketClientTests : IDisposable
{
    // private WebSocketServer? server;
    // private readonly int port;
    private HashSet<int> _usedPorts = new HashSet<int>();
    public WebSocketClientTests()
    {
    }
    private int GetPortRand()
    {
       lock (this)
        {
            int port = new Random().Next(50000, 60000);
            while (_usedPorts.Contains(port))
            {
                port = new Random().Next(50000, 60000);
            }
            _usedPorts.Add(port);
            return port;
        }
    }

    public void Dispose()
    {
    }

    #region Client Status Lifecycle (via Server)

    [Fact]
    public void Status_ShouldBeOpend_AfterUpgrade()
    {
        IWebSocketClient? connectedClient = null;
        var clientReady = new ManualResetEventSlim(false);
        int port = GetPortRand();
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            connectedClient = ctx.HttpUpgrade();
            connectedClient.OnClientClose += (_) => { };
            clientReady.Set();
        };
        server.Start();

        using var tcp = new TcpClient();
        tcp.Connect(IPAddress.Loopback, port);
        var stream = tcp.GetStream();
        var key = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
        var request = "GET / HTTP/1.1\r\n" +
                      $"Host: {IPAddress.Loopback}:{port}\r\n" +
                      "Upgrade: websocket\r\n" +
                      "Connection: Upgrade\r\n" +
                      $"Sec-WebSocket-Key: {key}\r\n" +
                      "Sec-WebSocket-Version: 13\r\n\r\n";
        stream.Write(Encoding.UTF8.GetBytes(request));
        stream.Flush();

        Assert.True(clientReady.Wait(TimeSpan.FromSeconds(5)));
        Assert.NotNull(connectedClient);
        Assert.Equal(LHZ.WebSocket.Enums.ClientStatus.Opend, connectedClient!.Status);

        tcp.Close();
        server.Stop();
    }

    [Fact]
    public void Client_Close_ShouldTransitionToCloseStatus()
    {
        IWebSocketClient? connectedClient = null;
        var clientReady = new ManualResetEventSlim(false);

        int port = GetPortRand();
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            connectedClient = ctx.HttpUpgrade();
            connectedClient.OnClientClose += (_) => { };
            clientReady.Set();
        };
        server.Start();

        using var tcp = new TcpClient();
        tcp.Connect(IPAddress.Loopback, port);
        var stream = tcp.GetStream();
        var key = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
        var request = "GET / HTTP/1.1\r\n" +
                      $"Host: {IPAddress.Loopback}:{port}\r\n" +
                      "Upgrade: websocket\r\n" +
                      "Connection: Upgrade\r\n" +
                      $"Sec-WebSocket-Key: {key}\r\n" +
                      "Sec-WebSocket-Version: 13\r\n\r\n";
        stream.Write(Encoding.UTF8.GetBytes(request));
        stream.Flush();

        Assert.True(clientReady.Wait(TimeSpan.FromSeconds(5)));

        connectedClient!.Close();
        Assert.Equal(LHZ.WebSocket.Enums.ClientStatus.Close, connectedClient.Status);

        server.Stop();
    }

    [Fact]
    public void Client_DoubleClose_ShouldNotThrow()
    {
        IWebSocketClient? connectedClient = null;
        var clientReady = new ManualResetEventSlim(false);

        int port = GetPortRand();
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            connectedClient = ctx.HttpUpgrade();
            connectedClient.OnClientClose += (_) => { };
            clientReady.Set();
        };
        server.Start();

        using var tcp = new TcpClient();
        tcp.Connect(IPAddress.Loopback, port);
        var stream = tcp.GetStream();
        var key = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
        var request = "GET / HTTP/1.1\r\n" +
                      $"Host: {IPAddress.Loopback}:{port}\r\n" +
                      "Upgrade: websocket\r\n" +
                      "Connection: Upgrade\r\n" +
                      $"Sec-WebSocket-Key: {key}\r\n" +
                      "Sec-WebSocket-Version: 13\r\n\r\n";
        stream.Write(Encoding.UTF8.GetBytes(request));
        stream.Flush();

        Assert.True(clientReady.Wait(TimeSpan.FromSeconds(5)));
        Thread.Sleep(300);

        // 第一次关闭
        connectedClient!.Close();
        // 第二次关闭不应抛出异常（Status == Close 时直接返回）
        var ex = Record.Exception(() => connectedClient.Close());
        Assert.Null(ex);

        server.Stop();
    }

    [Fact]
    public void Client_Dispose_ShouldClose()
    {
        IWebSocketClient? connectedClient = null;
        var clientReady = new ManualResetEventSlim(false);

        int port = GetPortRand();
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            connectedClient = ctx.HttpUpgrade();
            connectedClient.OnClientClose += (_) => { };
            clientReady.Set();
        };
        server.Start();

        using var tcp = new TcpClient();
        tcp.Connect(IPAddress.Loopback, port);
        var stream = tcp.GetStream();
        var key = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
        var request = "GET / HTTP/1.1\r\n" +
                      $"Host: {IPAddress.Loopback}:{port}\r\n" +
                      "Upgrade: websocket\r\n" +
                      "Connection: Upgrade\r\n" +
                      $"Sec-WebSocket-Key: {key}\r\n" +
                      "Sec-WebSocket-Version: 13\r\n\r\n";
        stream.Write(Encoding.UTF8.GetBytes(request));
        stream.Flush();

        Assert.True(clientReady.Wait(TimeSpan.FromSeconds(5)));
        connectedClient!.Dispose();
        Assert.Equal(LHZ.WebSocket.Enums.ClientStatus.Close, connectedClient.Status);

        server.Stop();
    }

    [Fact]
    public void Client_OnClientClose_ShouldBeRaised()
    {
        IWebSocketClient? connectedClient = null;
        var closeRaised = new ManualResetEventSlim(false);
        var clientReady = new ManualResetEventSlim(false);

        int port = GetPortRand();
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            connectedClient = ctx.HttpUpgrade();
            connectedClient.OnClientClose += (_) => closeRaised.Set();
            clientReady.Set();
        };
        server.Start();

        using var tcp = new TcpClient();
        tcp.Connect(IPAddress.Loopback, port);
        var stream = tcp.GetStream();
        var key = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
        var request = "GET / HTTP/1.1\r\n" +
                      $"Host: {IPAddress.Loopback}:{port}\r\n" +
                      "Upgrade: websocket\r\n" +
                      "Connection: Upgrade\r\n" +
                      $"Sec-WebSocket-Key: {key}\r\n" +
                      "Sec-WebSocket-Version: 13\r\n\r\n";
        stream.Write(Encoding.UTF8.GetBytes(request));
        stream.Flush();

        Assert.True(clientReady.Wait(TimeSpan.FromSeconds(5)));
        connectedClient!.Close();

        Assert.True(closeRaised.Wait(TimeSpan.FromSeconds(3)),
            "OnClientClose should be raised");

        server.Stop();
    }

    #endregion

    #region Factory Method Error Cases

    [Fact]
    public void CreateWebSocketClient_InvalidUrl_ShouldThrow()
    {
        Assert.Throws<Exception>(() =>
            WebSocketClient.CreateWebSocketClient("not-a-valid-url"));
    }

    [Fact]
    public void CreateWebSocketClient_NullUrl_ShouldThrow()
    {
        Assert.Throws<Exception>(() =>
            WebSocketClient.CreateWebSocketClient(""));
    }

    [Fact]
    public void CreateWebSocketClient_NonexistentServer_ShouldThrow()
    {
        Assert.Throws<SocketException>(() =>
            WebSocketClient.CreateWebSocketClient("ws://localhost:1/"));
    }

    #endregion
}

