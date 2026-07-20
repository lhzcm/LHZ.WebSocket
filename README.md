# LHZ.WebSocket.Server

一个基于 .NET 的轻量级 WebSocket 服务端库，从底层实现 WebSocket 协议（RFC 6455），不依赖任何第三方 WebSocket 框架。

## 项目结构

```
LHZ.WebSocket.Server/
├── LHZ.WebSocket.Server/          # 核心库
│   ├── WebSocketServer.cs         # WebSocket 服务端主类
│   ├── WebSocketClient.cs         # WebSocket 客户端连接封装
│   ├── Core/
│   │   ├── DataFrame.cs           # WebSocket 数据帧解析
│   │   └── WebSocketStream.cs     # WebSocket 流读取封装
│   ├── Enums/
│   │   └── Opcode.cs              # 操作码枚举
│   └── Http/
│       ├── HttpContext.cs         # HTTP 升级请求上下文
│       └── HttpRequest.cs         # HTTP 请求解析器
└── LHZ.WebSocket.TestConsole/     # 测试控制台项目
    └── Program.cs                 # 使用示例
```

## 功能特性

- ✅ **纯 .NET 实现**：基于 `TcpListener` / `TcpClient` 从零构建，无第三方依赖
- ✅ **HTTP 升级握手**：自动处理 HTTP → WebSocket 协议升级
- ✅ **数据帧解析**：支持 RFC 6455 标准的数据帧格式，包括 FIN、RSV、Opcode、Masked、Payload Length 等字段
- ✅ **分片消息**：支持跨多个数据帧的连续消息（Continuation Frame）
- ✅ **掩码处理**：支持客户端发送的掩码数据自动解码
- ✅ **多种操作码**：支持 Text、Binary、Close、Ping、Pong
- ✅ **事件驱动**：基于事件（Event）的异步回调模型
- ✅ **.NET 10**：目标框架为 `net10.0`

## 快速开始

### 安装

将 `LHZ.WebSocket.Server` 项目添加为你的项目引用：

```xml
<ItemGroup>
    <ProjectReference Include="..\LHZ.WebSocket.Server\LHZ.WebSocket.Server.csproj" />
</ItemGroup>
```

### 基本用法

```csharp
using LHZ.WebSocket.Server;
using LHZ.WebSocket.Server.Http;

// 创建 WebSocket 服务器，监听 5000 端口
WebSocketServer server = new WebSocketServer(5000);

// 处理升级请求（可用于鉴权、拒绝连接等）
server.OnUpgradeRequest += (HttpContext context) =>
{
    Console.WriteLine($"收到升级请求: {context.Request.Url}");
    return true; // 返回 true 允许升级，false 拒绝连接
};

// 处理客户端连接
server.OnClientConnected += (WebSocketClient client) =>
{
    Console.WriteLine("客户端已连接");

    // 处理消息接收
    client.OnMessageReceived += (WebSocketClient sender, byte[] message) =>
    {
        string text = System.Text.Encoding.UTF8.GetString(message);
        Console.WriteLine($"收到消息: {text}");
    };

    // 启动接收循环
    client.StartReceiving();
};

// 启动服务器
server.Start();
```

### 指定 IP 地址

```csharp
// 绑定到特定 IP
WebSocketServer server = new WebSocketServer(IPAddress.Parse("127.0.0.1"), 5000);
```

## API 参考

### WebSocketServer

| 成员 | 说明 |
|------|------|
| `WebSocketServer(int port)` | 创建服务器实例，监听所有网络接口 |
| `WebSocketServer(IPAddress ip, int port)` | 创建服务器实例，绑定指定 IP |
| `Start()` | 启动服务器，开始接受连接 |
| `OnUpgradeRequest` | 事件：HTTP 升级请求到达时触发，返回 `true` 允许升级 |
| `OnClientConnected` | 事件：客户端成功连接后触发 |

### WebSocketClient

| 成员 | 说明 |
|------|------|
| `StartReceiving()` | 开始接收消息（在 `OnClientConnected` 事件中调用） |
| `OnMessageReceived` | 事件：收到完整消息时触发，回调参数为 `(WebSocketClient client, byte[] message)` |

### Opcode 枚举

| 值 | 说明 |
|----|------|
| `Continuation = 0x0` | 连续帧 |
| `Text = 0x1` | 文本消息 |
| `Binary = 0x2` | 二进制消息 |
| `Close = 0x8` | 关闭连接 |
| `Ping = 0x9` | 心跳 Ping |
| `Pong = 0xA` | 心跳 Pong |

## 依赖

- .NET 10 SDK

## 许可

本项目基于 [MIT License](LICENSE) 开源。
