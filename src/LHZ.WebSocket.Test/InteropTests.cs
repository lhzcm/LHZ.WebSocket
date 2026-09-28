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
public class InteropTests
{
    private int GetPortRand() => TestPorts.GetFreePort();

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
            client.SendByte((byte[])payload.Clone());

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

            // 服务端应检测协议错误,先回 1002 Close 帧告知原因,再关闭连接
            var (opcode, fin, _, payload) = ReadFrameRaw(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.True(fin, "close frame must be a complete frame");
            Assert.Equal(2, payload.Length);
            Assert.Equal((int)CloseCode.ProtocolError, (payload[0] << 8) | payload[1]);

            // 之后连接应被关闭(读到 EOF)
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
            client.OnPingReceived += (IWebSocketClient sender, byte[] data) =>
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

    #region 已修复缺陷的回归测试

    /// <summary>
    /// 回归：超过 65535 字节的负载走 64 位长度字段，而发送路径用的是 ArrayPool 租来的
    /// 未清零缓冲区。长度字段高 4 字节曾未被写入，对端会读到一个荒谬的长度并错位。
    /// 这里直接检查线路上的 10 字节头部，逐字节校验。
    /// </summary>
    [Fact]
    public void LargePayload_ShouldWriteValid64BitLengthOnTheWire()
    {
        int port = GetPortRand();
        const int payloadLength = 70000;
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            var client = ctx.HttpUpgrade();
            client.OnMessageReceived += (IWebSocketClient sender, string msg) =>
            {
                sender.SendByte(new byte[payloadLength]);
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

            var mask = new byte[] { 0x0A, 0x0B, 0x0C, 0x0D };
            stream.Write(BuildFrame(OpCode.Text, true, mask, Encoding.UTF8.GetBytes("go")));
            stream.Flush();

            // 手工读头部,不走 ReadFrameRaw:必须亲自验证高 4 字节确实是 0
            var header = ReadExact(stream, 2);
            Assert.Equal((byte)OpCode.Binary, (byte)(header[0] & 0x0F));
            Assert.Equal(127, header[1] & 0x7F);

            var ext = ReadExact(stream, 8);
            Assert.Equal(0, ext[0]);
            Assert.Equal(0, ext[1]);
            Assert.Equal(0, ext[2]);
            Assert.Equal(0, ext[3]);

            long declaredLength = ((long)ext[0] << 56) | ((long)ext[1] << 48) | ((long)ext[2] << 40) | ((long)ext[3] << 32)
                | ((long)ext[4] << 24) | ((long)ext[5] << 16) | ((long)ext[6] << 8) | ext[7];
            Assert.Equal(payloadLength, declaredLength);

            // 负载长度也必须与声明一致,否则后续读取会错位
            var payload = ReadExact(stream, payloadLength);
            Assert.Equal(payloadLength, payload.Length);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：服务端曾不校验客户端帧必须带掩码(RFC 6455 §5.1)。
    /// 现在应回 1002 并断开。
    /// </summary>
    [Fact]
    public void UnmaskedClientFrame_ShouldBeRejectedWithProtocolError()
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

            // maskKey 传 null:未掩码的客户端帧
            stream.Write(BuildFrame(OpCode.Text, true, null, Encoding.UTF8.GetBytes("unmasked")));
            stream.Flush();

            var (opcode, _, _, payload) = ReadFrameRaw(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.Equal((int)CloseCode.ProtocolError, (payload[0] << 8) | payload[1]);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：RSV1-3 置位时必须失败连接,因为本实现不协商任何扩展(RFC 6455 §5.2)。
    /// 此前这些位被解析出来却从不校验。
    /// </summary>
    [Fact]
    public void ReservedBitSet_ShouldBeRejectedWithProtocolError()
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

            var mask = new byte[] { 0x21, 0x22, 0x23, 0x24 };
            var frame = BuildFrame(OpCode.Text, true, mask, Encoding.UTF8.GetBytes("rsv"));
            frame[0] |= 0x40; // 置 RSV1
            stream.Write(frame, 0, frame.Length);
            stream.Flush();

            var (opcode, _, _, payload) = ReadFrameRaw(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.Equal((int)CloseCode.ProtocolError, (payload[0] << 8) | payload[1]);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：文本帧负载不是合法 UTF-8 时必须以 1007 失败连接(RFC 6455 §8.1),
    /// 而不是静默产生替换字符。
    /// </summary>
    [Fact]
    public void InvalidUtf8TextFrame_ShouldBeRejectedWith1007()
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

            var mask = new byte[] { 0x31, 0x32, 0x33, 0x34 };
            // 0xC3 是一个 2 字节序列的首字节,单独出现即非法
            stream.Write(BuildFrame(OpCode.Text, true, mask, new byte[] { 0x48, 0xC3, 0x28 }));
            stream.Flush();

            var (opcode, _, _, payload) = ReadFrameRaw(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.Equal((int)CloseCode.InvalidFramePayloadData, (payload[0] << 8) | payload[1]);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：3000-4999 是 RFC 6455 §7.4 允许的已注册/私有关闭码,
    /// 此前枚举外的码一律被当成错误,对端用 4000 正常关闭会被误判。
    /// </summary>
    [Fact]
    public void PrivateRangeCloseCode_ShouldBeAccepted()
    {
        int port = GetPortRand();
        CloseCode? observed = null;
        var done = new ManualResetEventSlim(false);
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            var client = ctx.HttpUpgrade();
            client.OnCloseReceived += (IWebSocketClient sender, CloseMessage msg) =>
            {
                observed = msg.CloseCode;
                done.Set();
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

            var mask = new byte[] { 0x41, 0x42, 0x43, 0x44 };
            stream.Write(BuildFrame(OpCode.Close, true, mask, new byte[] { 0x0F, 0xA0 })); // 4000
            stream.Flush();

            Assert.True(done.Wait(TimeSpan.FromSeconds(5)), "close code 4000 should be delivered, not treated as an error");
            Assert.Equal(4000, (int)observed!.Value);

            // 回执应带同一个关闭码
            var (opcode, _, _, payload) = ReadFrameRaw(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.Equal(4000, (payload[0] << 8) | payload[1]);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：1005 是保留码,绝不能出现在线路上。
    /// 收到含 1005 的 Close 帧应以协议错误失败连接。
    /// </summary>
    [Fact]
    public void ReservedCloseCode_ShouldBeRejected()
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

            var mask = new byte[] { 0x51, 0x52, 0x53, 0x54 };
            stream.Write(BuildFrame(OpCode.Close, true, mask, new byte[] { 0x03, 0xED })); // 1005
            stream.Flush();

            var (opcode, _, _, payload) = ReadFrameRaw(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.Equal((int)CloseCode.ProtocolError, (payload[0] << 8) | payload[1]);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：收到空负载的 Close 帧时,回执不得把 1005 写回线路。
    /// </summary>
    [Fact]
    public void EmptyCloseFrame_AckMustNotEchoReservedCode()
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

            var mask = new byte[] { 0x61, 0x62, 0x63, 0x64 };
            stream.Write(BuildFrame(OpCode.Close, true, mask, Array.Empty<byte>()));
            stream.Flush();

            var (opcode, _, _, payload) = ReadFrameRaw(stream);
            Assert.Equal(OpCode.Close, opcode);
            // 空负载进 -> 空负载出,而不是把保留码 1005 写回去
            Assert.Empty(payload);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：收到 Ping 必须自动回 Pong 并带回同样的负载(RFC 6455 §5.5.2)。
    /// 此前需要用户自己接 OnPingReceived 手动回应,而 README 从未提到这点。
    /// </summary>
    [Fact]
    public void Ping_ShouldBeAnsweredWithPongAutomatically()
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

            var mask = new byte[] { 0x71, 0x72, 0x73, 0x74 };
            var pingPayload = Encoding.UTF8.GetBytes("heartbeat");
            stream.Write(BuildFrame(OpCode.Ping, true, mask, pingPayload));
            stream.Flush();

            var (opcode, fin, _, payload) = ReadFrameRaw(stream);
            Assert.Equal(OpCode.Pong, opcode);
            Assert.True(fin);
            Assert.Equal(pingPayload, payload);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：超过 125 字节的控制帧负载非法(RFC 6455 §5.5)。
    /// </summary>
    [Fact]
    public void OversizedControlFrame_ShouldBeRejected()
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

            var mask = new byte[] { 0x81, 0x02, 0x03, 0x04 };
            stream.Write(BuildFrame(OpCode.Ping, true, mask, new byte[126]));
            stream.Flush();

            var (opcode, _, _, payload) = ReadFrameRaw(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.Equal((int)CloseCode.ProtocolError, (payload[0] << 8) | payload[1]);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：分片消息没有大小上限时,对端只要一直不置 FIN 就能耗尽内存。
    /// 现在超过 MaxMessageSize 应以 1009 失败连接。
    /// </summary>
    [Fact]
    public void MessageExceedingMaxMessageSize_ShouldBeRejectedWith1009()
    {
        int port = GetPortRand();
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            var client = ctx.HttpUpgrade();
            client.MaxMessageSize = 100;
        };
        server.Start();
        try
        {
            using var tcp = new TcpClient();
            tcp.Connect(IPAddress.Loopback, port);
            var stream = tcp.GetStream();
            WriteUpgradeRequest(stream, port);
            Assert.Contains("101", ReadHttpResponseRaw(stream));

            var mask = new byte[] { 0x91, 0x92, 0x93, 0x94 };
            // 每片 60 字节,两片就越过 100 的上限
            stream.Write(BuildFrame(OpCode.Text, false, mask, new byte[60]));
            stream.Write(BuildFrame(OpCode.Continuation, false, mask, new byte[60]));
            stream.Flush();

            var (opcode, _, _, payload) = ReadFrameRaw(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.Equal((int)CloseCode.MessageTooBig, (payload[0] << 8) | payload[1]);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：OnClientConnected 事件曾被声明并写进 README,却从未被触发。
    /// </summary>
    [Fact]
    public void OnClientConnected_ShouldFireAfterHandshake()
    {
        int port = GetPortRand();
        var done = new ManualResetEventSlim(false);
        IWebSocketClient? connected = null;
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) => ctx.HttpUpgrade();
        server.OnClientConnected += (client) =>
        {
            connected = client;
            done.Set();
        };
        server.Start();
        try
        {
            using var tcp = new TcpClient();
            tcp.Connect(IPAddress.Loopback, port);
            var stream = tcp.GetStream();
            WriteUpgradeRequest(stream, port);
            Assert.Contains("101", ReadHttpResponseRaw(stream));

            Assert.True(done.Wait(TimeSpan.FromSeconds(5)), "OnClientConnected should fire after the handshake");
            Assert.NotNull(connected);
            Assert.Equal(ClientStatus.Opened, connected!.Status);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：Stop() 后服务器状态曾可能卡在 Closing,导致 Start() 拒绝重启。
    /// </summary>
    [Fact]
    public void ServerShouldBeRestartableAfterStop()
    {
        int port = GetPortRand();
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) => ctx.HttpUpgrade();
        server.Start();
        server.Stop();
        Assert.Equal(ServerStatus.Closed, server.Status);

        server.Start();
        try
        {
            Assert.Equal(ServerStatus.Start, server.Status);
            using var tcp = new TcpClient();
            tcp.Connect(IPAddress.Loopback, port);
            var stream = tcp.GetStream();
            WriteUpgradeRequest(stream, port);
            Assert.Contains("101", ReadHttpResponseRaw(stream));
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：Close() 曾是先检查后修改,多个线程可同时通过检查,
    /// 导致重复 dispose 并多次触发 OnClientClose。
    /// </summary>
    [Fact]
    public void ConcurrentClose_ShouldRaiseCloseEventExactlyOnce()
    {
        int port = GetPortRand();
        int closeCount = 0;
        var connected = new ManualResetEventSlim(false);
        IWebSocketClient? client = null;
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) =>
        {
            var c = ctx.HttpUpgrade();
            c.OnClientClose += (_) => Interlocked.Increment(ref closeCount);
        };
        server.OnClientConnected += (c) =>
        {
            client = c;
            connected.Set();
        };
        server.Start();
        try
        {
            using var tcp = new TcpClient();
            tcp.Connect(IPAddress.Loopback, port);
            var stream = tcp.GetStream();
            WriteUpgradeRequest(stream, port);
            Assert.Contains("101", ReadHttpResponseRaw(stream));
            Assert.True(connected.Wait(TimeSpan.FromSeconds(5)));

            // 8 个线程同时关闭同一个连接
            Parallel.For(0, 8, _ => client!.Close());

            Assert.Equal(ClientStatus.Close, client!.Status);
            Assert.Equal(1, Volatile.Read(ref closeCount));
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：CreateWebSocketClient 曾直接往调用方的 header 集合里塞 Host,
    /// 同一个集合复用第二次就会因重复头抛异常。
    /// </summary>
    [Fact]
    public void CreateWebSocketClient_ShouldNotMutateCallerHeaders()
    {
        int port = GetPortRand();
        WebSocketServer server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += (ctx) => ctx.HttpUpgrade();
        server.Start();
        try
        {
            var headers = new LHZ.WebSocket.Http.HttpHeaders();
            headers.TryAddWithoutValidation("X-Test", "1");

            using var first = WebSocketClient.CreateWebSocketClient($"ws://{IPAddress.Loopback}:{port}/", headers);
            first.Open();
            // 同一个集合再用一次:此前会因为 Host 重复而失败
            using var second = WebSocketClient.CreateWebSocketClient($"ws://{IPAddress.Loopback}:{port}/", headers);
            second.Open();

            Assert.False(headers.Contains("Host"), "the caller's header collection must not be modified");
            Assert.Equal(ClientStatus.Opened, first.Status);
            Assert.Equal(ClientStatus.Opened, second.Status);
        }
        finally
        {
            server.Stop();
        }
    }

    /// <summary>
    /// 回归：wss:// 没有 TLS 实现,以前会当成明文 443 端口去连,现在应明确报不支持。
    /// </summary>
    [Fact]
    public void CreateWebSocketClient_WssScheme_ShouldThrowNotSupported()
    {
        Assert.Throws<NotSupportedException>(() =>
            WebSocketClient.CreateWebSocketClient("wss://example.invalid/chat"));
    }

    #endregion
}
