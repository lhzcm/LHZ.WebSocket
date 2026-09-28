using System.Net;
using System.Net.Sockets;

namespace LHZ.WebSocket.Test;

/// <summary>
/// 为测试分配互不冲突的本地端口。
/// 各测试类此前各自用 new Random().Next(...) 猜端口,测试数量一多就会撞车,
/// 而且 xUnit 为每个 [Fact] 新建测试类实例,实例级的"已用端口"集合根本起不到去重作用。
/// 这里改为让操作系统分配一个当前空闲的端口,并在进程级记录已发出的端口。
/// </summary>
internal static class TestPorts
{
    private static readonly HashSet<int> _issued = new HashSet<int>();
    private static readonly object _lock = new object();

    /// <summary>取一个当前空闲、且本进程尚未发出过的本地端口。</summary>
    public static int GetFreePort()
    {
        lock (_lock)
        {
            for (int attempt = 0; attempt < 50; attempt++)
            {
                // 绑定到端口 0,由内核挑一个空闲端口,记下来再立刻释放。
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                int port = ((IPEndPoint)listener.LocalEndpoint).Port;
                listener.Stop();

                if (_issued.Add(port))
                {
                    return port;
                }
            }
            throw new InvalidOperationException("Could not obtain a free local port for the test.");
        }
    }
}
