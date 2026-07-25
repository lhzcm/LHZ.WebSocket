using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;

namespace LHZ.WebSocket.Http
{
    /// <summary>
    /// Reads and parses an HTTP request from a NetworkStream.
    /// Extracts the request line (method, URL, HTTP version) and all headers.
    /// </summary>
    public sealed class HttpRequest
    {
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

        private HttpRequest()
        {
            Headers = new HttpHeaders();
        }
        public HttpRequest(string url, string method, string httpVersion, System.Net.Http.Headers.HttpHeaders? headers = null)
        {
            Url = url;
            Method = method;
            HttpVersion = httpVersion;
            Headers = headers ?? new HttpHeaders();
        }
        public static HttpRequest GetRequestFromStream(Stream stream)
        {
            var httpRequest = new HttpRequest();
            httpRequest.Parse(stream);
            return httpRequest;
        }
        public void WriteToStream(Stream stream)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append(Method);
            stringBuilder.Append(' ');
            stringBuilder.Append(Url);
            stringBuilder.Append(' ');
            stringBuilder.Append(HttpVersion);
            stringBuilder.Append("\r\n");
            foreach(var item in Headers)
            {
                stringBuilder.Append(item.Key);
                stringBuilder.Append(": ");
                foreach(var value in item.Value)
                {
                    stringBuilder.Append(value);
                    stringBuilder.Append(',');
                }
                stringBuilder[stringBuilder.Length - 1] = '\n';
                stringBuilder.Insert(stringBuilder.Length - 1, '\r');
            }
            stringBuilder.Append("\r\n");
            var bytes = System.Text.Encoding.UTF8.GetBytes(stringBuilder.ToString());
            stream.Write(bytes, 0, bytes.Length);
            stream.Flush();
        }
        private void Parse(Stream stream)
        {
            // Read the request line: METHOD URL HTTP/VERSION
            string requestLine = ReadLine(stream);
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
            while (!string.IsNullOrEmpty(line = ReadLine(stream)))
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

        private string ReadLine(Stream stream)
        {
            var sb = new StringBuilder();
            int prev = -1;
            int cur;
            while ((cur = stream.ReadByte()) != -1)
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
}