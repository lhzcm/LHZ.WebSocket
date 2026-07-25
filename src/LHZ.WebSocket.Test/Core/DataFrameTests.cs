using LHZ.WebSocket.Core;
using LHZ.WebSocket.Enums;

namespace LHZ.WebSocket.Test.Core;

/// <summary>
/// 测试 DataFrame 的核心功能：帧创建、属性、掩码、头部序列化、分片等。
/// </summary>
public class DataFrameTests
{
    #region CreateDataFrame (OpCode, FIN, MaskingKey, Data)

    [Fact]
    public void CreateDataFrame_TextFrame_ShouldSetCorrectOpcodeAndFin()
    {
        var data = System.Text.Encoding.UTF8.GetBytes("Hello");
        var frame = DataFrame.CreateDataFrame(OpCode.Text, true, null, data);

        Assert.Equal(OpCode.Text, frame.Opcode);
        Assert.True(frame.FIN);
        Assert.False(frame.Masked);
        Assert.Equal(data, frame.Data.ToArray());
    }

    [Fact]
    public void CreateDataFrame_BinaryFrame_ShouldSetCorrectOpcode()
    {
        var data = new byte[] { 0x01, 0x02, 0x03, 0x04 };
        var frame = DataFrame.CreateDataFrame(OpCode.Binary, true, null, data);

        Assert.Equal(OpCode.Binary, frame.Opcode);
        Assert.True(frame.FIN);
        Assert.False(frame.Masked);
        Assert.Equal(data, frame.Data.ToArray());
    }

    [Fact]
    public void CreateDataFrame_CloseFrame_ShouldSetCorrectOpcode()
    {
        var data = new byte[] { 0x03, 0xE8 }; // 1000 = Normal
        var frame = DataFrame.CreateDataFrame(OpCode.Close, true, null, data);

        Assert.Equal(OpCode.Close, frame.Opcode);
        Assert.True(frame.FIN);
    }

    [Fact]
    public void CreateDataFrame_PingFrame_ShouldSetCorrectOpcode()
    {
        var data = new byte[] { 0x70, 0x69, 0x6E, 0x67 }; // "ping"
        var frame = DataFrame.CreateDataFrame(OpCode.Ping, true, null, data);

        Assert.Equal(OpCode.Ping, frame.Opcode);
        Assert.True(frame.FIN);
    }

    [Fact]
    public void CreateDataFrame_PongFrame_ShouldSetCorrectOpcode()
    {
        var data = new byte[] { 0x70, 0x6F, 0x6E, 0x67 }; // "pong"
        var frame = DataFrame.CreateDataFrame(OpCode.Pong, true, null, data);

        Assert.Equal(OpCode.Pong, frame.Opcode);
        Assert.True(frame.FIN);
    }

    [Fact]
    public void CreateDataFrame_ContinuationFrame_ShouldSetCorrectOpcode()
    {
        var data = new byte[] { 0x01, 0x02 };
        var frame = DataFrame.CreateDataFrame(OpCode.Continuation, false, null, data);

        Assert.Equal(OpCode.Continuation, frame.Opcode);
        Assert.False(frame.FIN);
    }

    [Fact]
    public void CreateDataFrame_WithMaskingKey_ShouldMaskPayload()
    {
        var original = new byte[] { 0x01, 0x02, 0x03, 0x04 };
        var maskingKey = new byte[] { 0x10, 0x20, 0x30, 0x40 };
        var frame = DataFrame.CreateDataFrame(OpCode.Binary, true, maskingKey, original);

        Assert.True(frame.Masked);
        Assert.Equal(maskingKey, frame.MaskingKey);

        // 验证 XOR 掩码: data[i] ^= maskingKey[i % 4]
        var expected = new byte[]
        {
            (byte)(0x01 ^ 0x10),
            (byte)(0x02 ^ 0x20),
            (byte)(0x03 ^ 0x30),
            (byte)(0x04 ^ 0x40),
        };
        Assert.Equal(expected, frame.Data.ToArray());
    }

    [Fact]
    public void CreateDataFrame_InvalidMaskingKeyLength_ShouldThrow()
    {
        var data = new byte[] { 0x01 };
        var badKey = new byte[] { 0x01, 0x02, 0x03 }; // 只有 3 字节

        Assert.Throws<ArgumentException>(() =>
            DataFrame.CreateDataFrame(OpCode.Text, true, badKey, data));
    }

    [Fact]
    public void CreateDataFrame_NullMaskingKey_ShouldNotBeMasked()
    {
        var data = new byte[] { 0x01, 0x02, 0x03 };
        var frame = DataFrame.CreateDataFrame(OpCode.Text, true, null, data);

        Assert.False(frame.Masked);
        Assert.Null(frame.MaskingKey);
        Assert.Equal(data, frame.Data.ToArray());
    }

    #endregion

    #region RSV Bits

    [Fact]
    public void CreateDataFrame_DefaultRsvBits_ShouldBeFalse()
    {
        var frame = DataFrame.CreateDataFrame(OpCode.Text, true, null, new byte[] { 0x01 });

        Assert.False(frame.RSV1);
        Assert.False(frame.RSV2);
        Assert.False(frame.RSV3);
    }

    #endregion

    #region DataFrameHeader

    [Fact]
    public void DataFrameHeader_SmallPayload_ShouldBe2Bytes()
    {
        var data = new byte[125]; // 125 字节（7-bit 范围最大值）
        var frame = DataFrame.CreateDataFrame(OpCode.Text, true, null, data);

        var header = frame.DataFrameHeader;
        Assert.Equal(2, header.Length);
        Assert.Equal(0x81, header[0]); // FIN + Text
        Assert.Equal(125, header[1]);  // payload length
    }

    [Fact]
    public void DataFrameHeader_SmallPayloadWithMask_ShouldBe6Bytes()
    {
        var data = new byte[10];
        var maskingKey = new byte[] { 0xAA, 0xBB, 0xCC, 0xDD };
        var frame = DataFrame.CreateDataFrame(OpCode.Text, true, maskingKey, data);

        var header = frame.DataFrameHeader;
        Assert.Equal(6, header.Length);  // 2 + 4 mask
        Assert.Equal(0x81, header[0]);   // FIN + Text
        Assert.Equal(0x8A, header[1]);   // masked (0x80) + len 10
        Assert.Equal(0xAA, header[2]);
        Assert.Equal(0xBB, header[3]);
        Assert.Equal(0xCC, header[4]);
        Assert.Equal(0xDD, header[5]);
    }

    [Fact]
    public void DataFrameHeader_MediumPayload126_ShouldBe4Bytes()
    {
        var data = new byte[126]; // 触发 16-bit 扩展长度
        var frame = DataFrame.CreateDataFrame(OpCode.Binary, true, null, data);

        var header = frame.DataFrameHeader;
        Assert.Equal(4, header.Length); // 2 + 2 extended
        Assert.Equal(0x82, header[0]);  // FIN + Binary
        Assert.Equal(126, header[1]);   // 126 标记
        Assert.Equal(0, header[2]);     // 高位 = 0
        Assert.Equal(126, header[3]);   // 低位 = 126
    }

    [Fact]
    public void DataFrameHeader_MediumPayload126WithMask_ShouldBe8Bytes()
    {
        var data = new byte[200];
        var maskingKey = new byte[] { 0x01, 0x02, 0x03, 0x04 };
        var frame = DataFrame.CreateDataFrame(OpCode.Text, true, maskingKey, data);

        var header = frame.DataFrameHeader;
        Assert.Equal(8, header.Length); // 2 + 2 extended + 4 mask
        Assert.Equal(254, header[1]);   // 126 | 0x80 (mask bit)
        Assert.Equal(0, header[2]);     // 高位
        Assert.Equal(200, header[3]);   // 低位
    }

    [Fact]
    public void DataFrameHeader_LargePayload65536_ShouldBe10Bytes()
    {
        var data = new byte[65536]; // 需要 64-bit 扩展长度
        var frame = DataFrame.CreateDataFrame(OpCode.Binary, true, null, data);

        var header = frame.DataFrameHeader;
        Assert.Equal(10, header.Length); // 2 + 8 extended
        Assert.Equal(0x82, header[0]);   // FIN + Binary
        Assert.Equal(127, header[1]);    // 127 标记
        // 前 4 字节为 0
        Assert.Equal(0, header[2]);
        Assert.Equal(0, header[3]);
        Assert.Equal(0, header[4]);
        Assert.Equal(0, header[5]);
        // 65536 = 0x00010000
        Assert.Equal(0, header[6]);
        Assert.Equal(1, header[7]);
        Assert.Equal(0, header[8]);
        Assert.Equal(0, header[9]);
    }

    #endregion

    #region Fragmentation via Stream

    [Fact]
    public void CreateDataFrame_FromStream_SmallData_ProducesSingleFrame()
    {
        var data = new byte[50];
        new Random(42).NextBytes(data);
        using var ms = new MemoryStream(data);

        var frames = DataFrame.CreateDataFrame(OpCode.Binary, null, ms, 100).ToList();

        Assert.Single(frames);
        Assert.True(frames[0].FIN);
        Assert.Equal(OpCode.Binary, frames[0].Opcode);
        Assert.Equal(data, frames[0].Data.ToArray());
    }

    [Fact]
    public void CreateDataFrame_FromStream_LargeData_ProducesMultipleFrames()
    {
        var data = new byte[250]; // > 100 buffer size
        new Random(42).NextBytes(data);
        using var ms = new MemoryStream(data);

        var frames = DataFrame.CreateDataFrame(OpCode.Text, null, ms, 100).ToList();

        Assert.Equal(3, frames.Count); // 100 + 100 + 50

        // 第一帧：非 FIN，Text
        Assert.False(frames[0].FIN);
        Assert.Equal(OpCode.Text, frames[0].Opcode);
        Assert.Equal(100, frames[0].Data.Count);

        // 第二帧：非 FIN，Continuation
        Assert.False(frames[1].FIN);
        Assert.Equal(OpCode.Continuation, frames[1].Opcode);
        Assert.Equal(100, frames[1].Data.Count);

        // 第三帧：FIN，Continuation
        Assert.True(frames[2].FIN);
        Assert.Equal(OpCode.Continuation, frames[2].Opcode);
        Assert.Equal(50, frames[2].Data.Count);

        // 拼接后数据一致
        var combined = frames.SelectMany(f => f.Data.ToArray()).ToArray();
        Assert.Equal(data, combined);
    }

    [Fact]
    public void CreateDataFrame_FromStream_ExactBufferSize_ProducesThreeFrames()
    {
        // 200 bytes with 100-byte buffer: reads 100→fragment, reads 100→fragment, reads 0→final empty
        var data = new byte[200];
        new Random(42).NextBytes(data);
        using var ms = new MemoryStream(data);

        var frames = DataFrame.CreateDataFrame(OpCode.Binary, null, ms, 100).ToList();

        Assert.Equal(3, frames.Count);

        Assert.False(frames[0].FIN);
        Assert.Equal(100, frames[0].Data.Count);

        Assert.False(frames[1].FIN);
        Assert.Equal(100, frames[1].Data.Count);

        Assert.True(frames[2].FIN);
        Assert.Empty(frames[2].Data.ToArray());

        var combined = frames.Take(2).SelectMany(f => f.Data.ToArray()).ToArray();
        Assert.Equal(data, combined);
    }

    [Fact]
    public void CreateDataFrame_FromStream_EmptyData_ProducesSingleEmptyFrame()
    {
        using var ms = new MemoryStream(Array.Empty<byte>());

        var frames = DataFrame.CreateDataFrame(OpCode.Text, null, ms).ToList();

        Assert.Single(frames);
        Assert.True(frames[0].FIN);
        Assert.Empty(frames[0].Data.ToArray());
    }

    [Fact]
    public void CreateDataFrame_FromStream_WithMasking_AllFramesShouldBeMasked()
    {
        var data = new byte[250];
        new Random(42).NextBytes(data);
        var maskingKey = new byte[] { 0x0F, 0x1E, 0x2D, 0x3C };
        using var ms = new MemoryStream(data);

        var frames = DataFrame.CreateDataFrame(OpCode.Text, maskingKey, ms, 100).ToList();

        Assert.All(frames, f => Assert.True(f.Masked));
        Assert.All(frames, f => Assert.Equal(maskingKey, f.MaskingKey));
    }

    #endregion

    #region Properties

    [Fact]
    public void DataDataFrameFlag_ShouldReflectFinAndOpcode()
    {
        var frame = DataFrame.CreateDataFrame(OpCode.Close, true, null, new byte[] { 0x03, 0xE8 });

        // FIN=1(0x80) + Close=0x8 => 0x88
        Assert.Equal(0x88, frame.DataDataFrameFlag);
    }

    [Fact]
    public void DataDataFrameFlag_NonFin_ShouldNotHaveFinBit()
    {
        var frame = DataFrame.CreateDataFrame(OpCode.Text, false, null, new byte[] { 0x41 });

        // FIN=0 + Text=0x1 => 0x01
        Assert.Equal(0x01, frame.DataDataFrameFlag);
    }

    #endregion
}
