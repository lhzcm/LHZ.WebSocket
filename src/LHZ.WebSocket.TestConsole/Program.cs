using LHZ.WebSocket;
using LHZ.WebSocket.Core;
using LHZ.WebSocket.Http;
using LHZ.WebSocket.Interfaces;

// var client = WebSocketClient.CreateWebSocketClient("ws://localhost:5000/");
// client.SendMessage("Hello World!");
// client.OnMessageReceived += (IWebSocketClient sender, string message) =>
// {
//     Console.WriteLine(message);
//     client.SendMessage($"client recive : {message}");
// };
// // Handle client-initiated close
// client.OnCloseRecived += (IWebSocketClient sender, CloseMessage message) =>
// {
//     Console.WriteLine(message);
//     sender.Close();
// };
// client.Open();
// Console.WriteLine("client is Start");
// Console.ReadLine();

// Create a WebSocket server on port 5000
WebSocketServer webSocketServer = new WebSocketServer(5000);

// Handle each incoming HTTP upgrade request
webSocketServer.OnUpgradeRequest += (HttpContext httpContext) =>
{
    httpContext.Response?.Headers?.Add("test", "true");
    var client = httpContext.HttpUpgrade();
    // Echo text messages back to the client
    client.OnMessageReceived += (IWebSocketClient sender, string message) =>
    {
        Console.WriteLine(message);
        client.SendMessage($"server recive : {message}");
    };
    // Handle client-initiated close
    client.OnCloseRecived += (IWebSocketClient sender, CloseMessage message) =>
    {
        Console.WriteLine(message);
        sender.Close();
    };
};

webSocketServer.Start();

Console.WriteLine("webSocketServer is Start");
Console.ReadLine();
webSocketServer.Stop();
Console.WriteLine("webSocketServer is Stop");