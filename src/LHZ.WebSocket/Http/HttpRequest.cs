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
        /// <summary>Creates a request to send.</summary>
        /// <param name="url">Request target, e.g. /chat.</param>
        /// <param name="method">HTTP method, e.g. GET.</param>
        /// <param name="httpVersion">HTTP version string, e.g. HTTP/1.1.</param>
        /// <param name="headers">Request headers; a new empty collection is used when null.</param>
        public HttpRequest(string url, string method, string httpVersion, System.Net.Http.Headers.HttpHeaders? headers = null)
        {
            Url = url;
            Method = method;
            HttpVersion = httpVersion;
            Headers = headers ?? new HttpHeaders();
        }
        /// <summary>
        /// Get Request Info From NetStream
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public static HttpRequest GetRequestFromStream(Stream stream)
        {
            var httpRequest = new HttpRequest();
            httpRequest.Parse(stream);
            return httpRequest;
        }
        /// <summary>Serializes the request line and headers to the stream and flushes it.</summary>
        /// <param name="stream">The destination stream.</param>
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
                // One line per value rather than a comma-joined list: comma joining is invalid
                // for headers such as Set-Cookie. A header with no values is skipped, which
                // also avoids indexing past the end of the buffer.
                foreach (var value in item.Value)
                {
                    stringBuilder.Append(item.Key);
                    stringBuilder.Append(": ");
                    stringBuilder.Append(value);
                    stringBuilder.Append("\r\n");
                }
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
                    try
                    {
                        Headers.Add(key, value);
                    }
                    catch(Exception)
                    {
                        Headers.TryAddWithoutValidation(key, value);
                    }
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