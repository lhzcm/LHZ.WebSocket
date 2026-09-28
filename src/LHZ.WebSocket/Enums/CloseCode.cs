namespace LHZ.WebSocket.Enums
{
    /// <summary>
    /// WebSocket close status codes (RFC 6455 Section 7.4).
    /// Codes 3000-3999 are registered with IANA and 4000-4999 are for private use;
    /// both ranges are accepted on the wire but have no named member here.
    /// </summary>
    public enum CloseCode
    {
        /// <summary>Normal Closure 正常关闭</summary>
        Normal = 1000,

        /// <summary>Going Away 端点离开，如浏览器关闭页面</summary>
        GoingAway = 1001,

        /// <summary>Protocol Error 协议错误</summary>
        ProtocolError = 1002,

        /// <summary>Unsupported Data 不支持的数据类型</summary>
        UnsupportedData = 1003,

        /// <summary>Reserved 保留，不得出现在线路上</summary>
        Reserved = 1004,

        /// <summary>No Status Received 未收到状态码，保留，不得发送</summary>
        NoStatusReceived = 1005,

        /// <summary>Abnormal Closure 异常关闭，保留，不得发送</summary>
        AbnormalClosure = 1006,

        /// <summary>Invalid Frame Payload Data 无效的帧负载数据，例如文本帧不是合法 UTF-8</summary>
        InvalidFramePayloadData = 1007,

        /// <summary>Policy Violation 策略违规</summary>
        PolicyViolation = 1008,

        /// <summary>Message Too Big 消息过大</summary>
        MessageTooBig = 1009,

        /// <summary>Mandatory Extension 缺少必需的扩展</summary>
        MandatoryExtension = 1010,

        /// <summary>Internal Server Error 服务器内部错误</summary>
        InternalServerError = 1011,

        /// <summary>Service Restart 服务重启</summary>
        ServiceRestart = 1012,

        /// <summary>Try Again Later 稍后重试</summary>
        TryAgainLater = 1013,

        /// <summary>Bad Gateway 错误的网关</summary>
        BadGateway = 1014,

        /// <summary>TLS Handshake 失败，保留，不得发送</summary>
        TlsHandshake = 1015,
    }
}
