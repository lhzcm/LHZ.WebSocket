using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using LHZ.WebSocket.Interfaces;

namespace LHZ.WebSocket.Test;

/// <summary>
/// 端到端互操作测试:
/// - 库客户端(WebSocketClient)与库服务端(WebSocketServer)互通(文本/二进制回显)
/// - 客户端发送帧必须带掩码(RFC 6455 §5.1),用原始 TCP 服务器验证
/// - 慢握手不得阻塞其他客户端的握手(accept 循环并发回归)
/// - 并发握手全部成功
/// - 握手校验(缺 Sec-WebSocket-Key / Sec-WebSocket-Version 拒绝)
/// - Close 帧自动回执
/// - 分片合法性(独立 Continuation 帧导致断开;分片消息中穿插控制帧正确重组)
/// </summary>
public class InteropTests : IDisposable
{
    private readonly HashSet<int> _usedPorts = new HashSet<int>();
    private readonly object _portLock = new object();

    public void Dispose()
    {
    }

    private int GetPortRand()
    {
        lock (_portLock)
        {
            int port = new Random().Next(30000, 49000);
            while (_usedPorts.Contains(port))
            {
                port = new Random().Next(50000, 60000);
            }
            _usedPorts.Add(port);
            return port;
        }
    }

    #region 手写协议辅助

    /// <summary>构造一个 WebSocket 帧(支持掩码、126/127 扩展长度)。</summary>
    private static byte[] BuildFrame(OpCode opcode, bool fin, byte[]? maskKey, byte[] payload)
    {
        int extLenBytes = payload.Length < 126 ? 0 : (payload.Length <= ushort.MaxValue ? 2 : 8);
        int headerLen = 2 + extLenBytes + (maskKey != null ? 4 : 0);
        var frame = new byte[headerLen + payload.Length];
        frame[0] = (byte)((fin ? 0x80 : 0x00) | (byte)opcode);

        int second = payload.Length < 126 ? payload.Length : (payload.Length <= ushort.MaxValue ? 126 : 127);
        if (maskKey != null)
        {
            second |= 0x80;
        }
        frame[1] = (byte)second;

        int offset = 2;
        if (extLenBytes == 2)
        {
            frame[2] = (byte)(payload.Length >> 8);
            frame[3] = (byte)payload.Length;
            offset = 4;
        }
        else if (extLenBytes == 8)
        {
            // 只支持 int.MaxValue 以内的长度,高 4 字节为 0
            frame[6] = (byte)(payload.Length >> 24);
            frame[7] = (byte)(payload.Length >> 16);
            frame[8] = (byte)(payload.Length >> 8);
            frame[9] = (byte)payload.Length;
            offset = 10;
        }

        if (maskKey != null)
        {
            Array.Copy(maskKey, 0, frame, offset, 4);
            offset += 4;
            for (int i = 0; i < payload.Length; i++)
            {
                frame[offset + i] = (byte)(payload[i] ^ maskKey[i % 4]);
            }
        }
        else
        {
            Array.Copy(payload, 0, frame, offset, payload.Length);
        }
        return frame;
    }

    /// <summary>构造 WebSocket 升级请求。version 传 null 表示省略该头;key 传空字符串表示省略该头。</summary>
    private static string BuildUpgradeRequest(int port, string? version = "13", string? key = null)
    {
        key ??= Convert.ToBase64String(Guid.NewGuid().ToByteArray());
        var sb = new StringBuilder();
        sb.Append("GET / HTTP/1.1\r\n");
        sb.Append($"Host: {IPAddress.Loopback}:{port}\r\n");
        sb.Append("Upgrade: websocket\r\n");
        sb.Append("Connection: Upgrade\r\n");
        if (!string.IsNullOrEmpty(key))
        {
            sb.Append($"Sec-WebSocket-Key: {key}\r\n");
        }
        if (version != null)
        {
            sb.Append($"Sec-WebSocket-Version: {version}\r\n");
        }
        sb.Append("\r\n");
        return sb.ToString();
    }

    private static void WriteUpgradeRequest(Stream stream, int port, string? version = "13", string? key = null)
    {
        var bytes = Encoding.UTF8.GetBytes(BuildUpgradeRequest(port, version, key));
        stream.Write(bytes, 0, bytes.Length);
        stream.Flush();
    }

    /// <summary>逐字节读取 HTTP 响应头(到 \r\n\r\n 为止),避免 StreamReader 缓冲污染后续帧读取。</summary>
    private static string ReadHttpResponseRaw(Stream stream)
    {
        var sb = new StringBuilder();
        var buf = new byte[1];
        int state = 0;
        while (state < 4)
        {
            int n = stream.Read(buf, 0, 1);
            if (n == 0)
            {
                break;
            }
            char c = (char)buf[0];
            sb.Append(c);
            state = state == 0 && c == '\r' ? 1
                  : state == 1 && c == '\n' ? 2
                  : state == 2 && c == '\r' ? 3
                  : state == 3 && c == '\n' ? 4
                  : 0;
        }
        return sb.ToString();
    }

    /// <summary>从流中读取一帧(自动解掩码)。</summary>
    private static (OpCode opcode, bool fin, bool masked, byte[] payload) ReadFrameRaw(Stream stream)
    {
        var header = ReadExact(stream, 2);
        bool fin = (header[0] & 0x80) != 0;
        var opcode = (OpCode)(header[0] & 0x0F);
        bool masked = (header[1] & 0x80) != 0;
        int len = header[1] & 0x7F;
        byte[]? maskKey = null;
        if (len == 126)
        {
            var ext = ReadExact(stream, 2);
            len = (ext[0] << 8) | ext[1];
        }
        else if (len == 127)
        {
            var ext = ReadExact(stream, 8);
            len = (ext[4] << 24) | (ext[5] << 16) | (ext[6] << 8) | ext[7];
        }
        if (masked)
        {
            maskKey = ReadExact(stream, 4);
        }
        var payload = ReadExact(stream, len);
        if (maskKey != null)
        {
            for (int i = 0; i < payload.Length; i++)
            {
                payload[i] ^= maskKey[i % 4];
            }
        }
        return (opcode, fin, masked, payload);
    }

    private static byte[] ReadExact(Stream stream, int count)
    {
        var buffer = new byte[count];
        int total = 0;
        while (total < count)
        {
            int n = stream.Read(buffer, total, count - total);
            if (n == 0)
            {
                throw new EndOfStreamException();
            }
            total += n;
        }
        return buffer;
    }

    #endregion

    #region 库客户端 ↔ 库服务端互通

    [Fact]
    public void ClientToServer_TextMessage_RoundTrip()
    {
        int port = GetPortRand();
        string? echo = null;
        var echoReceived = new ManualResetEventSlim(false);
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            var client = ctx.HttpUpgrade();
            client.OnMessageReceived += (IWebSocketClient sender, string msg) =>
            {
                sender.SendMessage($"ECHO: {msg}");
            };
        };
        server.Start();
        try
        {
            using var client = WebSocketClient.CreateWebSocketClient($"ws://127.0.0.1:{port}/");
            client.OnMessageReceived += (IWebSocketClient sender, string msg) =>
            {
                echo = msg;
                echoReceived.Set();
            };
            client.Open();
            client.SendMessage("Hello interop");

            Assert.True(echoReceived.Wait(TimeSpan.FromSeconds(5)),
                "client should receive the echo from the server");
            Assert.Equal("ECHO: Hello interop", echo);
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void ClientToServer_BinaryMessage_RoundTrip()
    {
        int port = GetPortRand();
        byte[]? echo = null;
        var echoReceived = new ManualResetEventSlim(false);
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            var client = ctx.HttpUpgrade();
            client.OnBytesReceived += (IWebSocketClient sender, byte[] data) =>
            {
                sender.SendByte(data);
            };
        };
        server.Start();
        try
        {
            using var client = WebSocketClient.CreateWebSocketClient($"ws://127.0.0.1:{port}/");
            client.OnBytesReceived += (IWebSocketClient sender, byte[] data) =>
            {
                echo = data;
                echoReceived.Set();
            };
            client.Open();
            var payload = new byte[] { 0x00, 0xDE, 0xAD, 0xBE, 0xEF, 0xFF };
            client.SendByte(payload);

            Assert.True(echoReceived.Wait(TimeSpan.FromSeconds(5)),
                "client should receive the binary echo from the server");
            Assert.Equal(payload, echo);
        }
        finally
        {
            server.Stop();
        }
    }

    #endregion

    #region 客户端掩码合规(RFC 6455 §5.1)

    [Fact]
    public void ClientFrames_ShouldBeMasked_WhenConnectingToRawServer()
    {
        int port = GetPortRand();
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        (OpCode opcode, bool fin, bool masked, byte[] payload)? received = null;
        var frameReceived = new ManualResetEventSlim(false);

        var serverTask = Task.Run(() =>
        {
            try
            {
                using var tcp = listener.AcceptTcpClient();
                var stream = tcp.GetStream();
                var requestText = ReadHttpResponseRaw(stream);
                var keyLine = requestText.Split('\n')
                    .FirstOrDefault(l => l.StartsWith("Sec-WebSocket-Key:", StringComparison.OrdinalIgnoreCase))
                    ?? throw new InvalidOperationException("no Sec-WebSocket-Key in request");
                var key = keyLine.Split(':', 2)[1].Trim();
                var accept = Convert.ToBase64String(SHA1.HashData(
                    Encoding.UTF8.GetBytes(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")));
                var resp = $"HTTP/1.1 101 Switching Protocols\r\n" +
                           "Upgrade: websocket\r\n" +
                           "Connection: Upgrade\r\n" +
                           $"Sec-WebSocket-Accept: {accept}\r\n\r\n";
                var respBytes = Encoding.UTF8.GetBytes(resp);
                stream.Write(respBytes, 0, respBytes.Length);
                stream.Flush();
                received = ReadFrameRaw(stream);
                frameReceived.Set();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"raw server error: {ex.Message}");
            }
        });

        try
        {
            using var client = WebSocketClient.CreateWebSocketClient($"ws://127.0.0.1:{port}/");
            client.Open();
            client.SendMessage("masked!");

            Assert.True(frameReceived.Wait(TimeSpan.FromSeconds(5)),
                "raw server should receive a frame from the library client");
            Assert.True(received!.Value.masked, "client frames MUST be masked per RFC 6455 §5.1");
            Assert.Equal(OpCode.Text, received.Value.opcode);
            Assert.True(received.Value.fin);
            Assert.Equal("masked!", Encoding.UTF8.GetString(received.Value.payload));
        }
        finally
        {
            listener.Stop();
            serverTask.Wait(TimeSpan.FromSeconds(2));
        }
    }

    #endregion

    #region 并发 / 慢握手回归

    [Fact]
    public void SlowHandshake_ShouldNotBlockOtherClients()
    {
        int port = GetPortRand();
        // 握手超时 5 秒:修复前,慢客户端会阻塞 accept 循环,其他客户端被拖住
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port, 5);
        var fastConnected = new ManualResetEventSlim(false);
        server.OnUpgradeRequest += (ctx) =>
        {
            ctx.HttpUpgrade();
            fastConnected.Set();
        };
        server.Start();
        try
        {
            // 慢客户端:连接但从不发送握手数据
            using var slow = new TcpClient();
            slow.Connect(IPAddress.Loopback, port);

            // 快客户端:正常握手
            using var fast = new TcpClient();
            fast.Connect(IPAddress.Loopback, port);
            var stream = fast.GetStream();
            WriteUpgradeRequest(stream, port);
            var response = ReadHttpResponseRaw(stream);

            Assert.True(fastConnected.Wait(TimeSpan.FromSeconds(3)),
                "fast client handshake should not be blocked by the slow client");
            Assert.Contains("101", response);
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void ConcurrentHandshakes_AllShouldSucceed()
    {
        const int count = 20;
        int port = GetPortRand();
        int connected = 0;
        var allConnected = new ManualResetEventSlim(false);
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            var client = ctx.HttpUpgrade();
            client.OnClientClose += (_) => { };
            if (Interlocked.Increment(ref connected) >= count)
            {
                allConnected.Set();
            }
        };
        server.Start();
        var clients = new List<TcpClient>();
        try
        {
            for (int i = 0; i < count; i++)
            {
                var tcp = new TcpClient();
                tcp.Connect(IPAddress.Loopback, port);
                clients.Add(tcp);
            }
            foreach (var tcp in clients)
            {
                WriteUpgradeRequest(tcp.GetStream(), port);
            }

            Assert.True(allConnected.Wait(TimeSpan.FromSeconds(15)),
                $"expected {count} handshakes, got {connected}");
            foreach (var tcp in clients)
            {
                Assert.Contains("101", ReadHttpResponseRaw(tcp.GetStream()));
            }
            Assert.True(SpinWait.SpinUntil(() => server.ClientNums == count, TimeSpan.FromSeconds(5)),
                $"expected ClientNums == {count}, got {server.ClientNums}");
        }
        finally
        {
            foreach (var t in clients)
            {
                t.Dispose();
            }
            server.Stop();
        }
    }

    #endregion

    #region 握手校验

    [Fact]
    public void Handshake_MissingSecWebSocketVersion_ShouldReturn400()
    {
        int port = GetPortRand();
        bool upgradeCalled = false;
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            upgradeCalled = true;
            // 模拟真实用法:尝试完成升级,校验失败时 HttpUpgrade 抛异常并已写入 400
            try
            {
                ctx.HttpUpgrade();
            }
            catch (Exception)
            {
            }
        };
        server.Start();
        try
        {
            using var tcp = new TcpClient();
            tcp.Connect(IPAddress.Loopback, port);
            var stream = tcp.GetStream();
            WriteUpgradeRequest(stream, port, version: null);
            var response = ReadHttpResponseRaw(stream);

            Assert.Contains("400", response);
            Assert.True(upgradeCalled, "OnUpgradeRequest should fire, rejection happens inside HttpUpgrade()");
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void Handshake_MissingSecWebSocketKey_ShouldReturn400()
    {
        int port = GetPortRand();
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            try
            {
                ctx.HttpUpgrade();
            }
            catch (Exception)
            {
            }
        };
        server.Start();
        try
        {
            using var tcp = new TcpClient();
            tcp.Connect(IPAddress.Loopback, port);
            var stream = tcp.GetStream();
            WriteUpgradeRequest(stream, port, key: "");
            var response = ReadHttpResponseRaw(stream);

            Assert.Contains("400", response);
        }
        finally
        {
            server.Stop();
        }
    }

    #endregion

    #region Close 自动回执 / 分片合法性

    [Fact]
    public void CloseFrame_Received_ShouldAutoReplyClose()
    {
        int port = GetPortRand();
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            var client = ctx.HttpUpgrade();
            client.OnClientClose += (_) => { };
        };
        server.Start();
        try
        {
            using var tcp = new TcpClient();
            tcp.Connect(IPAddress.Loopback, port);
            var stream = tcp.GetStream();
            WriteUpgradeRequest(stream, port);
            Assert.Contains("101", ReadHttpResponseRaw(stream));

            var closePayload = new byte[] { 0x03, 0xE8 }; // 1000 Normal
            var closeFrame = BuildFrame(OpCode.Close, true, new byte[] { 0x01, 0x02, 0x03, 0x04 }, closePayload);
            stream.Write(closeFrame, 0, closeFrame.Length);
            stream.Flush();

            var (opcode, fin, _, payload) = ReadFrameRaw(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.True(fin, "close ack must be a complete frame");
            Assert.Equal(closePayload, payload);
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void ContinuationFrame_WithoutStartedMessage_ShouldCloseConnection()
    {
        int port = GetPortRand();
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) => ctx.HttpUpgrade();
        server.Start();
        try
        {
            using var tcp = new TcpClient();
            tcp.Connect(IPAddress.Loopback, port);
            var stream = tcp.GetStream();
            WriteUpgradeRequest(stream, port);
            Assert.Contains("101", ReadHttpResponseRaw(stream));

            // 协议错误:没有进行中的分片消息就发送 Continuation 帧
            var contFrame = BuildFrame(OpCode.Continuation, true, new byte[] { 0x01, 0x02, 0x03, 0x04 },
                Encoding.UTF8.GetBytes("orphan"));
            stream.Write(contFrame, 0, contFrame.Length);
            stream.Flush();

            // 服务端应检测协议错误并关闭连接(读到 EOF)
            var buffer = new byte[1];
            var readTask = Task.Run(() => stream.Read(buffer, 0, 1));
            Assert.True(readTask.Wait(TimeSpan.FromSeconds(5)),
                "server should close the connection after a protocol error");
            Assert.Equal(0, readTask.Result);
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void FragmentedMessage_WithInterleavedPing_ShouldReassembleCorrectly()
    {
        int port = GetPortRand();
        string? receivedMessage = null;
        int pingCount = 0;
        var done = new ManualResetEventSlim(false);
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            var client = ctx.HttpUpgrade();
            client.OnMessageReceived += (IWebSocketClient sender, string msg) =>
            {
                receivedMessage = msg;
                done.Set();
            };
            client.OnPingRecived += (IWebSocketClient sender, byte[] data) =>
            {
                Interlocked.Increment(ref pingCount);
            };
        };
        server.Start();
        try
        {
            using var tcp = new TcpClient();
            tcp.Connect(IPAddress.Loopback, port);
            var stream = tcp.GetStream();
            WriteUpgradeRequest(stream, port);
            Assert.Contains("101", ReadHttpResponseRaw(stream));

            var mask = new byte[] { 0x11, 0x22, 0x33, 0x44 };
            // 分片消息中穿插一个 Ping 控制帧(RFC 6455 §5.4 允许)
            stream.Write(BuildFrame(OpCode.Text, false, mask, Encoding.UTF8.GetBytes("Hel")));
            stream.Write(BuildFrame(OpCode.Ping, true, mask, Encoding.UTF8.GetBytes("ping")));
            stream.Write(BuildFrame(OpCode.Continuation, true, mask, Encoding.UTF8.GetBytes("lo")));
            stream.Flush();

            Assert.True(done.Wait(TimeSpan.FromSeconds(5)),
                "fragmented message should be reassembled with interleaved control frame");
            Assert.Equal("Hello", receivedMessage);
            Assert.Equal(1, pingCount);
        }
        finally
        {
            server.Stop();
        }
    }

    #endregion
}
