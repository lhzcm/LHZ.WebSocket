using System.Text;
using LHZ.WebSocket.Core;
using LHZ.WebSocket.Enums;

namespace LHZ.WebSocket.Test.Core;

/// <summary>
/// 测试 DataFrameReader 的同步和异步帧读取功能。
/// </summary>
public class DataFrameReaderTests
{
    #region 辅助方法

    /// <summary>向流中写入一个 WebSocket 帧。</summary>
    private static void WriteFrameToStream(Stream ms, OpCode opcode, bool fin, byte[]? maskingKey, byte[] payload)
    {
        byte firstByte = (byte)((fin ? 0x80 : 0x00) | (byte)opcode);

        byte secondByte;
        byte[] extendedLen;
        if (payload.Length < 126)
        {
            secondByte = (byte)payload.Length;
            extendedLen = Array.Empty<byte>();
        }
        else if (payload.Length <= ushort.MaxValue)
        {
            secondByte = 126;
            extendedLen = new byte[] { (byte)((payload.Length >> 8) & 0xFF), (byte)(payload.Length & 0xFF) };
        }
        else
        {
            secondByte = 127;
            extendedLen = new byte[8];
            extendedLen[4] = (byte)((payload.Length >> 24) & 0xFF);
            extendedLen[5] = (byte)((payload.Length >> 16) & 0xFF);
            extendedLen[6] = (byte)((payload.Length >> 8) & 0xFF);
            extendedLen[7] = (byte)(payload.Length & 0xFF);
        }

        if (maskingKey != null)
            secondByte |= 0x80;

        ms.WriteByte(firstByte);
        ms.WriteByte(secondByte);
        if (extendedLen.Length > 0)
            ms.Write(extendedLen, 0, extendedLen.Length);

        if (maskingKey != null)
        {
            ms.Write(maskingKey, 0, 4);
            var maskedPayload = new byte[payload.Length];
            for (int i = 0; i < payload.Length; i++)
                maskedPayload[i] = (byte)(payload[i] ^ maskingKey[i % 4]);
            ms.Write(maskedPayload, 0, maskedPayload.Length);
        }
        else
        {
            ms.Write(payload, 0, payload.Length);
        }
    }

    /// <summary>构建包含数据帧 + Close 帧的完整流，使 Read() 正常终止。</summary>
    private static MemoryStream BuildStreamWithClose(OpCode opcode, bool fin, byte[]? maskingKey, byte[] payload)
    {
        var ms = new MemoryStream();
        WriteFrameToStream(ms, opcode, fin, maskingKey, payload);
        var closePayload = new byte[] { 0x03, 0xE8 }; // 1000 Normal
        WriteFrameToStream(ms, OpCode.Close, true, null, closePayload);
        ms.Position = 0;
        return ms;
    }

    #endregion

    #region Sync Read

    [Fact]
    public void Read_SingleTextFrame_ShouldYieldFrameThenStopAtClose()
    {
        var payload = Encoding.UTF8.GetBytes("Hello, WebSocket!");
        using var ms = BuildStreamWithClose(OpCode.Text, true, null, payload);

        var reader = new DataFrameReader(ms);
        var frames = reader.Read().ToList();

        Assert.Equal(2, frames.Count);
        Assert.Equal(OpCode.Text, frames[0].Opcode);
        Assert.True(frames[0].FIN);
        Assert.Equal(payload, frames[0].Data.ToArray());
        Assert.Equal(OpCode.Close, frames[1].Opcode);
    }

    [Fact]
    public void Read_SingleBinaryFrame_ShouldYieldFrameThenStopAtClose()
    {
        var payload = new byte[] { 0x00, 0xFF, 0xAB, 0xCD };
        using var ms = BuildStreamWithClose(OpCode.Binary, true, null, payload);

        var reader = new DataFrameReader(ms);
        var frames = reader.Read().ToList();

        Assert.Equal(2, frames.Count);
        Assert.Equal(OpCode.Binary, frames[0].Opcode);
        Assert.Equal(payload, frames[0].Data.ToArray());
    }

    [Fact]
    public void Read_MaskedFrame_ShouldUnmaskCorrectly()
    {
        var payload = new byte[] { 0x48, 0x65, 0x6C, 0x6C, 0x6F }; // "Hello"
        var maskingKey = new byte[] { 0x37, 0xFA, 0x21, 0x3D };
        using var ms = BuildStreamWithClose(OpCode.Text, true, maskingKey, payload);

        var reader = new DataFrameReader(ms);
        var frames = reader.Read().ToList();

        Assert.Equal(payload, frames[0].Data.ToArray());
    }

    [Fact]
    public void Read_MultipleFrames_BeforeClose_ShouldYieldAll()
    {
        var ms = new MemoryStream();
        var p1 = Encoding.UTF8.GetBytes("Frame1");
        var p2 = new byte[] { 0xAA, 0xBB };
        WriteFrameToStream(ms, OpCode.Text, true, null, p1);
        WriteFrameToStream(ms, OpCode.Binary, true, null, p2);
        WriteFrameToStream(ms, OpCode.Close, true, null, new byte[] { 0x03, 0xE8 });
        ms.Position = 0;

        var reader = new DataFrameReader(ms);
        var frames = reader.Read().ToList();

        Assert.Equal(3, frames.Count);
        Assert.Equal(OpCode.Text, frames[0].Opcode);
        Assert.Equal(OpCode.Binary, frames[1].Opcode);
        Assert.Equal(OpCode.Close, frames[2].Opcode);
    }

    [Fact]
    public void Read_LargePayload126_ShouldReadCorrectly()
    {
        var payload = new byte[300];
        new Random(42).NextBytes(payload);
        using var ms = BuildStreamWithClose(OpCode.Binary, true, null, payload);

        var reader = new DataFrameReader(ms);
        var frames = reader.Read().ToList();

        Assert.Equal(payload, frames[0].Data.ToArray());
    }

    [Fact]
    public void Read_LargePayload65536_ShouldReadCorrectly()
    {
        var payload = new byte[65536];
        new Random(42).NextBytes(payload);
        using var ms = BuildStreamWithClose(OpCode.Binary, true, null, payload);

        var reader = new DataFrameReader(ms);
        var frames = reader.Read().ToList();

        Assert.Equal(payload, frames[0].Data.ToArray());
    }

    [Fact]
    public void Read_EmptyStream_ThrowsEndOfStreamException()
    {
        using var ms = new MemoryStream(Array.Empty<byte>());
        var reader = new DataFrameReader(ms);

        Assert.Throws<EndOfStreamException>(() => reader.Read().ToList());
    }

    [Fact]
    public void Read_TruncatedStream_ThrowsEndOfStreamException()
    {
        using var ms = new MemoryStream(new byte[] { 0x81, 0x05 });
        var reader = new DataFrameReader(ms);

        Assert.Throws<EndOfStreamException>(() => reader.Read().ToList());
    }

    #endregion

    #region Async Read

    [Fact]
    public async Task ReadAsync_SingleTextFrame_ShouldYieldOneFrame()
    {
        var payload = Encoding.UTF8.GetBytes("Async Hello!");
        var ms = new MemoryStream();
        WriteFrameToStream(ms, OpCode.Text, true, null, payload);
        ms.Position = 0;

        var reader = new DataFrameReader(ms);
        var frames = new List<DataFrame>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        try
        {
            await foreach (var frame in reader.ReadAsync(cts.Token))
            {
                frames.Add(frame);
                cts.Cancel(); // 读完一帧后主动取消
            }
        }
        catch (OperationCanceledException) { }

        Assert.Single(frames);
        Assert.Equal(OpCode.Text, frames[0].Opcode);
        Assert.Equal(payload, frames[0].Data.ToArray());
    }

    [Fact]
    public async Task ReadAsync_MultipleFrames_ShouldYieldAll()
    {
        var p1 = Encoding.UTF8.GetBytes("Frame1");
        var p2 = new byte[] { 0xAA, 0xBB };
        var p3 = Encoding.UTF8.GetBytes("Frame3");

        var ms = new MemoryStream();
        WriteFrameToStream(ms, OpCode.Text, true, null, p1);
        WriteFrameToStream(ms, OpCode.Binary, true, null, p2);
        WriteFrameToStream(ms, OpCode.Text, true, null, p3);
        ms.Position = 0;

        var reader = new DataFrameReader(ms);
        var frames = new List<DataFrame>();
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        try
        {
            await foreach (var frame in reader.ReadAsync(cts.Token))
            {
                frames.Add(frame);
                if (frames.Count >= 3) cts.Cancel();
            }
        }
        catch (OperationCanceledException) { }

        Assert.Equal(3, frames.Count);
        Assert.Equal(OpCode.Text, frames[0].Opcode);
        Assert.Equal(OpCode.Binary, frames[1].Opcode);
        Assert.Equal(OpCode.Text, frames[2].Opcode);
    }

    [Fact]
    public async Task ReadAsync_MaskedFrame_ShouldUnmaskCorrectly()
    {
        var payload = Encoding.UTF8.GetBytes("MaskedData");
        var maskingKey = new byte[] { 0x12, 0x34, 0x56, 0x78 };
        var ms = new MemoryStream();
        WriteFrameToStream(ms, OpCode.Text, true, maskingKey, payload);
        ms.Position = 0;

        var reader = new DataFrameReader(ms);
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var frames = new List<DataFrame>();

        try
        {
            await foreach (var frame in reader.ReadAsync(cts.Token))
            {
                frames.Add(frame);
                cts.Cancel();
            }
        }
        catch (OperationCanceledException) { }

        Assert.Single(frames);
        Assert.Equal(payload, frames[0].Data.ToArray());
    }

    [Fact]
    public async Task ReadAsync_ImmediateCancellation_ShouldStopWithoutFrames()
    {
        var payload = Encoding.UTF8.GetBytes("test");
        var ms = new MemoryStream();
        WriteFrameToStream(ms, OpCode.Text, true, null, payload);
        ms.Position = 0;

        var reader = new DataFrameReader(ms);
        var cts = new CancellationTokenSource();
        cts.Cancel();

        var frames = new List<DataFrame>();

        try
        {
            await foreach (var frame in reader.ReadAsync(cts.Token))
            {
                frames.Add(frame);
            }
            // 已取消的 token 应在首次 MoveNextAsync 时检测到并抛出
        }
        catch (OperationCanceledException)
        {
            Assert.Empty(frames);
            return;
        }

        Assert.Empty(frames); // 不应该到达这里，但如果没抛出，至少没有帧被读取
    }

    #endregion
}

