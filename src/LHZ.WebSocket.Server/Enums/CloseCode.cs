namespace LHZ.WebSocket.Server.Enums;

public enum CloseCode
{
    /// <summary>正常关闭 (Normal Closure)</summary>
    Normal = 1000,

    /// <summary>端点离开 (Going Away)，如浏览器关闭页面</summary>
    GoingAway = 1001,

    /// <summary>协议错误 (Protocol Error)</summary>
    ProtocolError = 1002,

    /// <summary>不支持的数据类型 (Unsupported Data)</summary>
    UnsupportedData = 1003,

    /// <summary>保留 (Reserved)，不应在 Close 帧中发送</summary>
    Reserved = 1004,

    /// <summary>未收到状态码 (No Status Received)，保留，不应发送</summary>
    NoStatusReceived = 1005,

    /// <summary>异常关闭 (Abnormal Closure)，保留，不应发送</summary>
    AbnormalClosure = 1006,

    /// <summary>无效的帧负载数据 (Invalid Frame Payload Data)</summary>
    InvalidFramePayloadData = 1007,

    /// <summary>策略违规 (Policy Violation)</summary>
    PolicyViolation = 1008,

    /// <summary>消息过大 (Message Too Big)</summary>
    MessageTooBig = 1009,

    /// <summary>缺少必需的扩展 (Mandatory Extension)</summary>
    MandatoryExtension = 1010,

    /// <summary>服务器内部错误 (Internal Server Error)</summary>
    InternalServerError = 1011,

    /// <summary>服务重启 (Service Restart)</summary>
    ServiceRestart = 1012,

    /// <summary>稍后重试 (Try Again Later)</summary>
    TryAgainLater = 1013,

    /// <summary>错误的网关 (Bad Gateway)</summary>
    BadGateway = 1014,

    /// <summary>TLS 握手失败 (TLS Handshake)，保留，不应发送</summary>
    TlsHandshake = 1015,
}
