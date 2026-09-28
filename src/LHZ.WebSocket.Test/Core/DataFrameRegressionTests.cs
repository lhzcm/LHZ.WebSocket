using System.Buffers;
using LHZ.WebSocket.Core;
using LHZ.WebSocket.Enums;

namespace LHZ.WebSocket.Test.Core;

/// <summary>
/// 针对已修复缺陷的回归测试。
/// 每个测试都对应一个真实存在过的 bug，请勿在不理解其意图的情况下放宽断言。
/// </summary>
public class DataFrameRegressionTests
{
    /// <summary>
    /// 回归：64 位长度分支曾漏写下标 2-5。
    /// 由于 DataFrameHeader 每次新分配一个已清零的数组，该缺陷被掩盖；
    /// 真实发送路径用的是 ArrayPool.Rent，池不清零，于是长度字段高 4 字节是残留垃圾。
    /// 这里刻意传入一个填满 0xFF 的缓冲区来暴露它。
    /// </summary>
    [Fact]
    public void DataFrameHeaderFull_LargePayload_ShouldWriteEveryHeaderByte()
    {
        var payload = new byte[ushort.MaxValue + 1];
        var frame = DataFrame.CreateDataFrame(OpCode.Binary, true, payload);

        Assert.Equal(10, frame.DataFrameHeaderLength);

        // 模拟 ArrayPool.Rent 返回的脏缓冲区
        var dirty = new byte[frame.DataFrameHeaderLength];
        for (int i = 0; i < dirty.Length; i++)
        {
            dirty[i] = 0xFF;
        }
        frame.DataFrameHeaderFull(ref dirty);

        Assert.Equal(127, dirty[1] & 0x7F);
        // 负载长度是 int，64 位长度字段的高 4 字节必须被显式写成 0
        Assert.Equal(0, dirty[2]);
        Assert.Equal(0, dirty[3]);
        Assert.Equal(0, dirty[4]);
        Assert.Equal(0, dirty[5]);

        long declaredLength = ((long)dirty[2] << 56) | ((long)dirty[3] << 48) | ((long)dirty[4] << 40) | ((long)dirty[5] << 32)
            | ((long)dirty[6] << 24) | ((long)dirty[7] << 16) | ((long)dirty[8] << 8) | dirty[9];
        Assert.Equal(payload.Length, declaredLength);
    }

    /// <summary>
    /// 回归：同一场景走真实的 ArrayPool，确保池里的脏数据不会泄漏进线路。
    /// </summary>
    [Fact]
    public void DataFrameHeaderFull_WithPooledBuffer_ShouldDeclareCorrectLength()
    {
        var frame = DataFrame.CreateDataFrame(OpCode.Binary, true, new byte[70000]);
        int headerLength = frame.DataFrameHeaderLength;

        // 先污染池：租出一块并填满非零值再归还
        var dirty = ArrayPool<byte>.Shared.Rent(headerLength);
        Array.Fill(dirty, (byte)0xAB);
        ArrayPool<byte>.Shared.Return(dirty);

        var rented = ArrayPool<byte>.Shared.Rent(headerLength);
        try
        {
            frame.DataFrameHeaderFull(ref rented);
            long declaredLength = ((long)rented[2] << 56) | ((long)rented[3] << 48) | ((long)rented[4] << 40) | ((long)rented[5] << 32)
                | ((long)rented[6] << 24) | ((long)rented[7] << 16) | ((long)rented[8] << 8) | rented[9];
            Assert.Equal(70000, declaredLength);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>
    /// ApplyMask 的循环上界曾写成 i &lt; data.Count 而非 i &lt; data.Offset + data.Count。
    /// 注意：当前所有公开工厂方法产出的段偏移都是 0，所以那个缺陷是潜伏的、走不到的；
    /// 本测试守住的是分片流帧逐帧掩码的正确性，以及掩码键按负载内序号取字节的语义。
    /// </summary>
    [Fact]
    public void CreateDataFrame_MaskedStream_ShouldMaskEveryPayloadByte()
    {
        // 每帧 4 字节，制造多个分片；最后一帧的段长度小于其在缓冲区中的位置
        var original = new byte[] { 10, 20, 30, 40, 50, 60 };
        const UInt32 maskingKey = 0x04030201;

        var frames = DataFrame.CreateDataFrame(OpCode.Binary, new MemoryStream(original), maskingKey, 4).ToList();

        // 逐帧解掩码后应还原出原始字节序列
        var restored = new List<byte>();
        foreach (var frame in frames)
        {
            Assert.True(frame.Masked);
            var segment = frame.Data;
            for (int i = 0; i < segment.Count; i++)
            {
                // 掩码键字节由该字节在负载内的序号决定
                byte keyByte = (byte)(maskingKey >> ((i % 4) << 3));
                restored.Add((byte)(segment.Array![segment.Offset + i] ^ keyByte));
            }
        }
        Assert.Equal(original, restored);
    }

    /// <summary>
    /// 回归：掩码必须按字节在负载内的序号取键，与其在底层数组中的下标无关。
    /// </summary>
    [Fact]
    public void CreateDataFrame_Masked_ShouldRoundTripThroughReader()
    {
        var payload = System.Text.Encoding.UTF8.GetBytes("mask round trip payload");
        const UInt32 maskingKey = 0xDEADBEEF;

        var frame = DataFrame.CreateDataFrame(OpCode.Text, true, (byte[])payload.Clone(), maskingKey);

        var stream = new MemoryStream();
        var header = frame.DataFrameHeader;
        stream.Write(header, 0, header.Length);
        stream.Write(frame.Data.Array!, frame.Data.Offset, frame.Data.Count);
        stream.Position = 0;

        var parsed = new DataFrameReader(stream).Read().First();
        Assert.True(parsed.Masked);
        Assert.Equal(payload, parsed.Data.ToArray());
    }
}
