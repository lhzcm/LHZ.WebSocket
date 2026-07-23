using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;

namespace LHZ.WebSocket.Server.Http;

/// <summary>
/// Reads and parses an HTTP request from a NetworkStream.
/// Extracts the request line (method, URL, HTTP version) and all headers.
/// </summary>
public sealed class HttpRequest
{
    private readonly NetworkStream _stream;

    /// <summary>
    /// HTTP method (e.g., GET, POST).
    /// </summary>
    public string Method { get; private set; } = string.Empty;

    /// <summary>
    /// Request URL/path (e.g., /chat).
    /// </summary>
    public string Url { get; private set; } = string.Empty;

    /// <summary>
    /// HTTP version string (e.g., HTTP/1.1).
    /// </summary>
    public string HttpVersion { get; private set; } = string.Empty;

    /// <summary>
    /// Parsed request headers (case-insensitive keys).
    /// </summary>
    public System.Net.Http.Headers.HttpHeaders Headers { get; private set; }

    public HttpRequest(NetworkStream stream)
    {
        _stream = stream;
        Headers = new HttpHeaders();
        Parse();
    }

    private void Parse()
    {
        // Read the request line: METHOD URL HTTP/VERSION
        string requestLine = ReadLine();
        if (string.IsNullOrEmpty(requestLine))
            throw new InvalidOperationException("Empty HTTP request.");

        var parts = requestLine.Split(' ');
        if (parts.Length < 3)
            throw new InvalidOperationException($"Invalid HTTP request line: {requestLine}");

        Method = parts[0];
        Url = parts[1];
        HttpVersion = parts[2];

        // Read headers until empty line
        string line;
        while (!string.IsNullOrEmpty(line = ReadLine()))
        {
            int colonIndex = line.IndexOf(':');
            if (colonIndex > 0)
            {
                string key = line.Substring(0, colonIndex).Trim();
                string value = line.Substring(colonIndex + 1).Trim();
                Headers.Add(key, value);
            }
        }
    }

    private string ReadLine()
    {
        var sb = new StringBuilder();
        int prev = -1;
        int cur;
        while ((cur = _stream.ReadByte()) != -1)
        {
            if (prev == '\r' && cur == '\n')
            {
                sb.Length--; // Remove trailing \r
                return sb.ToString();
            }
            sb.Append((char)cur);
            prev = cur;
        }
        return sb.ToString();
    }
}
