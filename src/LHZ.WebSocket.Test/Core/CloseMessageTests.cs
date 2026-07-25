using LHZ.WebSocket.Core;
using LHZ.WebSocket.Enums;

namespace LHZ.WebSocket.Test.Core;

/// <summary>
/// 测试 CloseMessage 结构体。
/// </summary>
public class CloseMessageTests
{
    [Fact]
    public void Constructor_ShouldSetProperties()
    {
        var msg = new CloseMessage(CloseCode.Normal, "Goodbye");

        Assert.Equal(CloseCode.Normal, msg.CloseCode);
        Assert.Equal("Goodbye", msg.Message);
    }

    [Fact]
    public void Constructor_EmptyMessage_ShouldWork()
    {
        var msg = new CloseMessage(CloseCode.GoingAway, "");

        Assert.Equal(CloseCode.GoingAway, msg.CloseCode);
        Assert.Equal("", msg.Message);
    }

    [Fact]
    public void Constructor_AllCloseCodes_ShouldWork()
    {
        foreach (CloseCode code in Enum.GetValues<CloseCode>())
        {
            var msg = new CloseMessage(code, "test");
            Assert.Equal(code, msg.CloseCode);
        }
    }

    [Fact]
    public void DefaultStruct_ShouldHaveDefaultValues()
    {
        var msg = default(CloseMessage);

        Assert.Equal(default(CloseCode), msg.CloseCode);
        Assert.Null(msg.Message);
    }
}
