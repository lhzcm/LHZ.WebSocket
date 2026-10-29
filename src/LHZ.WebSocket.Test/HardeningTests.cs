using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;
using LHZ.WebSocket.Exceptions;

namespace LHZ.WebSocket.Test;

/// <summary>
/// 针对"分析项目查找问题并修复"一轮修复的回归测试:
/// - 帧头声称的长度必须在分配缓冲区之前被拒绝(内存攻击面)
/// - 非最小长度编码 / 64 位长度最高位 / 控制帧超长 一律是协议错误
/// - 握手阶段的行长度、头数量上限
/// - 关闭后发送必须抛出,而不是悄悄入队直至永久阻塞
/// - Close() 发送 Close 帧(可带状态码与原因)
/// - 用户事件处理器抛异常不得断开连接
/// - 对端直接断开 TCP 不算错误
/// - CreateDataFrame(string) 产生的帧必须带 FIN
/// </summary>
public class HardeningTests
{
    private static int GetPort() => TestPorts.GetFreePort();

    #region 辅助

    private static MemoryStream StreamOf(params byte[] bytes) => new MemoryStream(bytes);

    private static byte[] MaskKey => new byte[] { 0x11, 0x22, 0x33, 0x44 };

    private static byte[] ClientFrame(OpCode opcode, bool fin, byte[] payload)
    {
        var key = MaskKey;
        var frame = new byte[6 + payload.Length];
        frame[0] = (byte)((fin ? 0x80 : 0) | (byte)opcode);
        frame[1] = (byte)(0x80 | payload.Length);
        Array.Copy(key, 0, frame, 2, 4);
        for (int i = 0; i < payload.Length; i++)
        {
            frame[6 + i] = (byte)(payload[i] ^ key[i % 4]);
        }
        return frame;
    }

    private static void Handshake(NetworkStream stream, int port, string? extraHeaders = null)
    {
        var request = "GET / HTTP/1.1\r\n" +
                      $"Host: 127.0.0.1:{port}\r\n" +
                      "Upgrade: websocket\r\nConnection: Upgrade\r\n" +
                      "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n" +
                      extraHeaders + "\r\n";
        var bytes = Encoding.UTF8.GetBytes(request);
        stream.Write(bytes, 0, bytes.Length);
        stream.Flush();
    }

    private static string ReadHttpHead(Stream stream)
    {
        var sb = new StringBuilder();
        var buf = new byte[1];
        while (!sb.ToString().EndsWith("\r\n\r\n"))
        {
            if (stream.Read(buf, 0, 1) == 0)
            {
                break;
            }
            sb.Append((char)buf[0]);
        }
        return sb.ToString();
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

    /// <summary>读一个服务端发来的(未掩码)帧。</summary>
    private static (OpCode opcode, byte[] payload) ReadServerFrame(Stream stream)
    {
        var header = ReadExact(stream, 2);
        int len = header[1] & 0x7F;
        if (len == 126)
        {
            var ext = ReadExact(stream, 2);
            len = (ext[0] << 8) | ext[1];
        }
        return ((OpCode)(header[0] & 0x0F), ReadExact(stream, len));
    }

    private static TcpClient ConnectRaw(int port)
    {
        var tcp = new TcpClient();
        tcp.Connect(IPAddress.Loopback, port);
        tcp.ReceiveTimeout = 5000;
        return tcp;
    }

    #endregion

    #region DataFrame

    [Fact]
    public void CreateDataFrame_String_ShouldSetFin()
    {
        using var frame = DataFrame.CreateDataFrame("hello");

        Assert.True(frame.FIN);
        Assert.Equal(OpCode.Text, frame.Opcode);
        Assert.Equal(0x81, frame.DataFrameHeader[0]);
    }

    #endregion

    #region 帧头长度校验(Reader)

    [Fact]
    public void Reader_NonMinimal16BitLength_ShouldBeProtocolError()
    {
        // 126 标记却只有 10 字节载荷
        var bytes = new byte[] { 0x82, 126, 0x00, 0x0A }.Concat(new byte[10]).ToArray();
        var reader = new DataFrameReader(StreamOf(bytes));

        var ex = Assert.Throws<WebSocketProtocolException>(() => reader.Read().First());
        Assert.Equal(CloseCode.ProtocolError, ex.CloseCode);
    }

    [Fact]
    public void Reader_NonMinimal64BitLength_ShouldBeProtocolError()
    {
        var bytes = new byte[] { 0x82, 127, 0, 0, 0, 0, 0, 0, 0, 0x0A }.Concat(new byte[10]).ToArray();
        var reader = new DataFrameReader(StreamOf(bytes));

        var ex = Assert.Throws<WebSocketProtocolException>(() => reader.Read().First());
        Assert.Equal(CloseCode.ProtocolError, ex.CloseCode);
    }

    [Fact]
    public void Reader_64BitLengthWithTopBitSet_ShouldBeProtocolError()
    {
        var bytes = new byte[] { 0x82, 127, 0x80, 0, 0, 0, 0, 0, 0, 0 };
        var reader = new DataFrameReader(StreamOf(bytes));

        var ex = Assert.Throws<WebSocketProtocolException>(() => reader.Read().First());
        Assert.Equal(CloseCode.ProtocolError, ex.CloseCode);
    }

    [Fact]
    public void Reader_64BitLengthBeyondInt32_ShouldBeMessageTooBig()
    {
        var bytes = new byte[] { 0x82, 127, 0, 0, 0, 1, 0, 0, 0, 0 };
        var reader = new DataFrameReader(StreamOf(bytes));

        var ex = Assert.Throws<WebSocketProtocolException>(() => reader.Read().First());
        Assert.Equal(CloseCode.MessageTooBig, ex.CloseCode);
    }

    [Fact]
    public async Task Reader_Async_NonMinimalLength_ShouldBeProtocolError()
    {
        var bytes = new byte[] { 0x82, 126, 0x00, 0x0A }.Concat(new byte[10]).ToArray();
        var reader = new DataFrameReader(StreamOf(bytes));

        var ex = await Assert.ThrowsAsync<WebSocketProtocolException>(async () =>
        {
            await foreach (var frame in reader.ReadAsync())
            {
                frame.Dispose();
            }
        });
        Assert.Equal(CloseCode.ProtocolError, ex.CloseCode);
    }

    [Fact]
    public void Reader_ControlFrameLongerThan125_ShouldBeRejectedFromTheHeaderAlone()
    {
        // Ping 声称 200 字节(用 126 标记,合法编码),但后面一个字节都没有:
        // 若先分配/读取载荷,会抛 EndOfStreamException 而不是协议错误
        var reader = new DataFrameReader(StreamOf(0x89, 126, 0x00, 0xC8));

        var ex = Assert.Throws<WebSocketProtocolException>(() => reader.Read().First());
        Assert.Equal(CloseCode.ProtocolError, ex.CloseCode);
    }

    [Fact]
    public void Reader_ValidMaximumControlFrame_ShouldStillBeAccepted()
    {
        var bytes = new byte[] { 0x89, 125 }.Concat(new byte[125]).ToArray();
        var reader = new DataFrameReader(StreamOf(bytes));

        using var frame = reader.Read().First();
        Assert.Equal(OpCode.Ping, frame.Opcode);
        Assert.Equal(125, frame.Data.Length);
    }

    #endregion

    #region 内存攻击面(端到端)

    /// <summary>
    /// 帧头声称 1 GiB 载荷但不发送任何载荷:服务端必须仅凭帧头就以 1009 失败连接,
    /// 而不是先 Rent 1 GiB 再傻等。
    /// </summary>
    [Fact]
    public void FrameHeaderClaimingHugePayload_ShouldBeRejectedWith1009Immediately()
    {
        int port = GetPort();
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        server.Start();
        try
        {
            using var tcp = ConnectRaw(port);
            var stream = tcp.GetStream();
            Handshake(stream, port);
            Assert.Contains("101", ReadHttpHead(stream));

            var header = new byte[] { 0x82, 0xFF, 0, 0, 0, 0, 0x40, 0, 0, 0, 0x11, 0x22, 0x33, 0x44 };
            stream.Write(header, 0, header.Length);
            stream.Flush();

            var (opcode, payload) = ReadServerFrame(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.Equal((int)CloseCode.MessageTooBig, (payload[0] << 8) | payload[1]);
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void HandshakeLineLongerThanLimit_ShouldGet431()
    {
        int port = GetPort();
        var errors = new List<Exception>();
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnError += errors.Add;
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        server.Start();
        try
        {
            using var tcp = ConnectRaw(port);
            var stream = tcp.GetStream();
            Handshake(stream, port, "X-Long: " + new string('a', 9000) + "\r\n");

            Assert.Contains("431", ReadHttpHead(stream));
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void HandshakeWithTooManyHeaders_ShouldGet431()
    {
        int port = GetPort();
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnError += _ => { };
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        server.Start();
        try
        {
            using var tcp = ConnectRaw(port);
            var stream = tcp.GetStream();
            var many = string.Concat(Enumerable.Range(0, 150).Select(i => $"X-H{i}: v\r\n"));
            Handshake(stream, port, many);

            Assert.Contains("431", ReadHttpHead(stream));
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void HandshakeWithinLimits_ShouldStillSucceed()
    {
        int port = GetPort();
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        server.Start();
        try
        {
            using var tcp = ConnectRaw(port);
            var stream = tcp.GetStream();
            // 8000 字节的单个头,加上 80 个小头,都在上限之内
            var extra = "X-Big: " + new string('a', 8000) + "\r\n" +
                        string.Concat(Enumerable.Range(0, 80).Select(i => $"X-H{i}: v\r\n"));
            Handshake(stream, port, extra);

            Assert.Contains("101", ReadHttpHead(stream));
        }
        finally
        {
            server.Stop();
        }
    }

    #endregion

    #region 关闭后发送

    [Fact]
    public async Task Send_AfterClose_ShouldThrowInsteadOfQueueingForever()
    {
        int port = GetPort();
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        server.Start();
        try
        {
            var client = WebSocketClient.CreateWebSocketClient($"ws://127.0.0.1:{port}/", null, 5, 4);
            client.Open();
            client.Close();

            // 容量只有 4:此前第 5 次同步发送会永远阻塞
            Assert.Throws<ChannelClosedException>(() => client.SendMessage("x"));
            await Assert.ThrowsAsync<ChannelClosedException>(() => client.SendMessageAsync("x"));
            await Assert.ThrowsAsync<ChannelClosedException>(() => client.PingAsync(Array.Empty<byte>()));
        }
        finally
        {
            server.Stop();
        }
    }

    #endregion

    #region Close 发送 Close 帧

    [Fact]
    public void Close_ShouldSendNormalCloseFrameToPeer()
    {
        int port = GetPort();
        IWebSocketClient? serverSide = null;
        var connected = new ManualResetEventSlim(false);
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        server.OnClientConnected += c => { serverSide = c; connected.Set(); };
        server.Start();
        try
        {
            using var tcp = ConnectRaw(port);
            var stream = tcp.GetStream();
            Handshake(stream, port);
            Assert.Contains("101", ReadHttpHead(stream));
            Assert.True(connected.Wait(5000));

            serverSide!.Close();

            var (opcode, payload) = ReadServerFrame(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.Equal((int)CloseCode.Normal, (payload[0] << 8) | payload[1]);
            Assert.Equal(ClientStatus.Close, serverSide.Status);
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void Close_WithCodeAndReason_ShouldSendThemToPeer()
    {
        int port = GetPort();
        IWebSocketClient? serverSide = null;
        var connected = new ManualResetEventSlim(false);
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        server.OnClientConnected += c => { serverSide = c; connected.Set(); };
        server.Start();
        try
        {
            using var tcp = ConnectRaw(port);
            var stream = tcp.GetStream();
            Handshake(stream, port);
            Assert.Contains("101", ReadHttpHead(stream));
            Assert.True(connected.Wait(5000));

            serverSide!.Close(CloseCode.GoingAway, "维护中");

            var (opcode, payload) = ReadServerFrame(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.Equal((int)CloseCode.GoingAway, (payload[0] << 8) | payload[1]);
            Assert.Equal("维护中", Encoding.UTF8.GetString(payload, 2, payload.Length - 2));
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void Close_WithReasonLongerThan123Bytes_ShouldThrowArgumentException()
    {
        int port = GetPort();
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        server.Start();
        try
        {
            using var client = WebSocketClient.CreateWebSocketClient($"ws://127.0.0.1:{port}/");
            client.Open();

            Assert.Throws<ArgumentException>(() => client.Close(CloseCode.Normal, new string('a', 124)));
            Assert.Equal(ClientStatus.Opened, client.Status);
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void Close_CalledTwice_ShouldSendOnlyOneCloseFrame()
    {
        int port = GetPort();
        IWebSocketClient? serverSide = null;
        var connected = new ManualResetEventSlim(false);
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        server.OnClientConnected += c => { serverSide = c; connected.Set(); };
        server.Start();
        try
        {
            using var tcp = ConnectRaw(port);
            var stream = tcp.GetStream();
            Handshake(stream, port);
            Assert.Contains("101", ReadHttpHead(stream));
            Assert.True(connected.Wait(5000));

            serverSide!.Close();
            serverSide.Close(CloseCode.GoingAway);

            var (opcode, payload) = ReadServerFrame(stream);
            Assert.Equal(OpCode.Close, opcode);
            Assert.Equal((int)CloseCode.Normal, (payload[0] << 8) | payload[1]);
            // 之后连接应直接结束,不会再有第二个 Close 帧
            Assert.Throws<EndOfStreamException>(() => ReadServerFrame(stream));
        }
        finally
        {
            server.Stop();
        }
    }

    #endregion

    #region 事件处理器异常 / 对端断线

    [Fact]
    public void ThrowingMessageHandler_ShouldBeReportedAndKeepConnectionOpen()
    {
        int port = GetPort();
        var errors = new List<Exception>();
        var errorSeen = new ManualResetEventSlim(false);
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx =>
        {
            var client = ctx.HttpUpgrade();
            client.OnError += (_, ex) => { lock (errors) { errors.Add(ex); } errorSeen.Set(); };
            client.OnMessageReceived += (sender, msg) =>
            {
                if (msg == "boom")
                {
                    throw new InvalidOperationException("handler bug");
                }
                sender.SendMessage("echo:" + msg);
            };
        };
        server.Start();
        try
        {
            using var tcp = ConnectRaw(port);
            var stream = tcp.GetStream();
            Handshake(stream, port);
            Assert.Contains("101", ReadHttpHead(stream));

            stream.Write(ClientFrame(OpCode.Text, true, Encoding.UTF8.GetBytes("boom")));
            Assert.True(errorSeen.Wait(5000), "the handler exception should surface through OnError");

            // 连接仍然可用
            stream.Write(ClientFrame(OpCode.Text, true, Encoding.UTF8.GetBytes("after")));
            var (opcode, payload) = ReadServerFrame(stream);
            Assert.Equal(OpCode.Text, opcode);
            Assert.Equal("echo:after", Encoding.UTF8.GetString(payload));
            lock (errors)
            {
                Assert.Single(errors);
                Assert.IsType<InvalidOperationException>(errors[0]);
            }
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void PeerDroppingTcp_ShouldCloseWithoutRaisingOnError()
    {
        int port = GetPort();
        var errors = new List<Exception>();
        var closed = new ManualResetEventSlim(false);
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx =>
        {
            var client = ctx.HttpUpgrade();
            client.OnError += (_, ex) => { lock (errors) { errors.Add(ex); } };
            client.OnClientClose += _ => closed.Set();
        };
        server.Start();
        try
        {
            var tcp = ConnectRaw(port);
            var stream = tcp.GetStream();
            Handshake(stream, port);
            Assert.Contains("101", ReadHttpHead(stream));

            // 不发 Close 帧,直接掐断连接
            tcp.Client.LingerState = new LingerOption(true, 0);
            tcp.Dispose();

            Assert.True(closed.Wait(5000), "OnClientClose should fire when the peer disappears");
            Thread.Sleep(100);
            lock (errors)
            {
                Assert.Empty(errors);
            }
        }
        finally
        {
            server.Stop();
        }
    }

    #endregion

    #region 文本直接编码进帧缓冲

    /// <summary>
    /// SendMessage 把文本直接编码进池化帧缓冲(先 GetByteCount 定长度,再 GetBytes 写入)。
    /// 空串、多字节、代理对、孤立代理项,两次计算必须一致,否则帧会错位。
    /// </summary>
    [Theory]
    [InlineData("")]
    [InlineData("hello")]
    [InlineData("héllo 你好 😀")]
    [InlineData("lone-high-surrogate-\uD83D")]
    public async Task SendMessage_Text_ShouldRoundTripExactly(string text)
    {
        int port = GetPort();
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx =>
        {
            var client = ctx.HttpUpgrade();
            client.OnMessageReceived += (sender, message) => sender.SendMessage(message);
        };
        server.Start();
        try
        {
            using var client = WebSocketClient.CreateWebSocketClient($"ws://127.0.0.1:{port}/");
            var echoed = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            client.OnMessageReceived += (_, message) => echoed.TrySetResult(message);
            client.Open();

            await client.SendMessageAsync(text);

            var result = await echoed.Task.WaitAsync(TimeSpan.FromSeconds(5));
            // 孤立的高代理项在 UTF-8 编码时被替换为 U+FFFD,往返后得到的就是替换后的文本。
            var expected = Encoding.UTF8.GetString(Encoding.UTF8.GetBytes(text));
            Assert.Equal(expected, result);
        }
        finally
        {
            server.Stop();
        }
    }

    [Theory]
    [InlineData(125)]
    [InlineData(126)]
    [InlineData(65535)]
    [InlineData(65536)]
    [InlineData(200000)]
    public void SendMessage_AtLengthEncodingBoundaries_ShouldRoundTripExactly(int length)
    {
        int port = GetPort();
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx =>
        {
            var client = ctx.HttpUpgrade();
            client.MaxMessageSize = 0;
            client.OnMessageReceived += (sender, message) => sender.SendMessage(message);
        };
        server.Start();
        try
        {
            using var client = WebSocketClient.CreateWebSocketClient($"ws://127.0.0.1:{port}/");
            client.MaxMessageSize = 0;
            var echoed = new ManualResetEventSlim(false);
            string? result = null;
            client.OnMessageReceived += (_, message) => { result = message; echoed.Set(); };
            client.Open();

            // 末尾放一个多字节字符,使字节数比字符数多 1,正好跨越长度编码的边界
            var text = new string('a', length - 1) + "é";
            client.SendMessage(text);

            Assert.True(echoed.Wait(5000));
            Assert.Equal(text, result);
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void SendMessage_Null_ShouldThrowAndLeaveConnectionOpen()
    {
        int port = GetPort();
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        server.Start();
        try
        {
            using var client = WebSocketClient.CreateWebSocketClient($"ws://127.0.0.1:{port}/");
            client.Open();

            Assert.Throws<ArgumentNullException>(() => client.SendMessage(null!));
            Assert.Equal(ClientStatus.Opened, client.Status);
        }
        finally
        {
            server.Stop();
        }
    }

    #endregion

    #region 服务端重启

    [Fact]
    public void Server_RepeatedStopStart_ShouldAlwaysEndInTheRequestedState()
    {
        int port = GetPort();
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        for (int i = 0; i < 20; i++)
        {
            server.Start();
            Assert.Equal(ServerStatus.Start, server.Status);
            server.Stop();
            Assert.Equal(ServerStatus.Closed, server.Status);
        }

        server.Start();
        try
        {
            using var client = WebSocketClient.CreateWebSocketClient($"ws://127.0.0.1:{port}/");
            client.Open();
            Assert.Equal(ServerStatus.Start, server.Status);
        }
        finally
        {
            server.Stop();
        }
    }

    [Fact]
    public void Server_Stop_ShouldCloseClientsAndSendCloseFrames()
    {
        int port = GetPort();
        var connected = new ManualResetEventSlim(false);
        var server = new WebSocketServer(IPAddress.Loopback, port);
        server.OnUpgradeRequest += ctx => ctx.HttpUpgrade();
        server.OnClientConnected += _ => connected.Set();
        server.Start();

        using var tcp = ConnectRaw(port);
        var stream = tcp.GetStream();
        Handshake(stream, port);
        Assert.Contains("101", ReadHttpHead(stream));
        Assert.True(connected.Wait(5000));

        server.Stop();

        var (opcode, payload) = ReadServerFrame(stream);
        Assert.Equal(OpCode.Close, opcode);
        Assert.Equal((int)CloseCode.Normal, (payload[0] << 8) | payload[1]);
        Assert.Equal(0, server.ClientNums);
    }

    #endregion
}
