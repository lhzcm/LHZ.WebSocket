using System.Buffers;
using LHZ.WebSocket.Http;

namespace LHZ.WebSocket.Test;

/// <summary>
/// 回归：异步发送在 channel 已关闭、或等待入队时被取消的情况下曾直接丢弃
/// <see cref="DataFrame"/>，其池化缓冲区永远不会归还 ArrayPool。
/// 这些测试利用 ArrayPool 按 size bucket 后进先出（LIFO）的特性，直接验证
/// 发送路径确实把缓冲区还回了池里：先租出并归还一个探针，随后一次 Rent
/// 必定返回同一个实例，除非该缓冲区在中间被漏掉了。
/// 集合禁止并行，保证没有别的测试同时访问同一个 bucket。
/// </summary>
[Collection(nameof(ArrayPoolLeaseCollection))]
public class ArrayPoolLeaseTests
{
    /// <summary>70,000 字节负载使用 10 字节的 64 位长度头。</summary>
    private const int PayloadLength = 70_000;
    private const int FrameLength = PayloadLength + 10;

    [Fact]
    public void SendMessageAsync_AfterClose_ShouldReturnFrameBufferToPool()
    {
        using var client = new WebSocketClient(new FakeHttpContext(), 4);
        // 未 Open 的连接：Close 直接完成发送 channel，之后任何发送都会失败。
        client.Close();

        byte[] probe = RentProbe();

        var send = client.SendMessageAsync(new string('a', PayloadLength));

        // channel 已关闭，写入必须同步失败，而不是把帧留在队列里。
        Assert.True(send.IsFaulted);
        _ = send.Exception; // 观察掉异常，避免 UnobservedTaskException

        Assert.Same(probe, RentProbe());
    }

    [Fact]
    public void SendMessageAsync_CancelledWhileQueueFull_ShouldReturnFrameBufferToPool()
    {
        // 容量 1：先塞入一个小帧把队列占满，异步写就会进入等待状态。
        using var client = new WebSocketClient(new FakeHttpContext(), 1);
        client.SendMessage("x");

        byte[] probe = RentProbe();

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var send = client.SendMessageAsync(new string('a', PayloadLength), cts.Token);

        // 队列满 + 已取消：帧不会被 channel 接收。
        Assert.True(send.IsCanceled);
        _ = send.Exception;

        Assert.Same(probe, RentProbe());
        // 取消一次发送不得关闭连接。
        Assert.NotEqual(ClientStatus.Close, client.Status);
    }

    /// <summary>
    /// 租出一个实例并立即归还：它随后位于该 bucket 的池顶，同线程、无并发时
    /// 下一次同 bucket 的 Rent 就是它。返回值用于身份比较。
    /// </summary>
    private static byte[] RentProbe()
    {
        byte[] rented = ArrayPool<byte>.Shared.Rent(FrameLength);
        ArrayPool<byte>.Shared.Return(rented);
        return rented;
    }

    private sealed class FakeHttpContext : IHttpContext
    {
        private readonly MemoryStream _stream = new MemoryStream();

        public HttpRequest Request { get; } = new HttpRequest("/", "GET", "HTTP/1.1", new HttpHeaders());
        public HttpResponse? Response => null;
        public HttpContextStatus Status => HttpContextStatus.Upgraded;
        public Stream Stream => _stream;

        public WebSocketClient HttpUpgrade(int capacity = 1024) => throw new NotSupportedException();
        public void Dispose() => _stream.Dispose();
    }
}

/// <summary>独占一个不允许并行运行的 xUnit 集合，供访问进程级 ArrayPool 的测试使用。</summary>
[CollectionDefinition(nameof(ArrayPoolLeaseCollection), DisableParallelization = true)]
public class ArrayPoolLeaseCollection
{
}
