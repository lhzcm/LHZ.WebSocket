using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace LHZ.WebSocket.Http
{
    public class HttpResponse
    {
        /// <summary>
        /// HttpStatusCode (e.g., 200).
        /// </summary>
        public HttpStatusCode StatusCode { get; private set; }

        /// <summary>
        /// HTTP version string (e.g., HTTP/1.1).
        /// </summary>
        public string HttpVersion { get; private set; } = string.Empty;

        /// <summary>
        /// Parsed request headers (case-insensitive keys).
        /// </summary>
        public System.Net.Http.Headers.HttpHeaders Headers { get; private set; }
        private HttpResponse()
        {
            Headers = new HttpHeaders();
        }
        public HttpResponse(HttpStatusCode statusCode, string httpVersion, System.Net.Http.Headers.HttpHeaders? headers = null)
        {
            StatusCode = statusCode;
            HttpVersion = httpVersion;
            Headers = headers ?? new HttpHeaders();
        }
        public static HttpResponse GetRequestFromStream(Stream stream)
        {
            var httpResponse = new HttpResponse();
            httpResponse.Parse(stream);
            return httpResponse;
        }
        public void WriteToStream(Stream stream)
        {
            var statusCodeName = new StringBuilder(StatusCode.ToString());
            for(int i = statusCodeName.Length - 1; i >= 0; i--)
            {
                if(statusCodeName[i] >= 'A' && statusCodeName[i] <= 'Z')
                {
                    statusCodeName.Insert(i, ' ');
                }
            }
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append(HttpVersion);
            stringBuilder.Append(' ');
            stringBuilder.Append(((int)StatusCode).ToString());
            stringBuilder.Append(' ');
            stringBuilder.Append(statusCodeName);
            stringBuilder.Append("\r\n");
            foreach (var item in Headers)
            {
                stringBuilder.Append(item.Key);
                stringBuilder.Append(": ");
                foreach (var value in item.Value)
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

            HttpVersion = parts[0];
            if(!Enum.TryParse<HttpStatusCode>(parts[1], out HttpStatusCode httpStatusCode))
            {
                throw new InvalidOperationException($"Invalid HTTP Status Code: {parts[1]}");
            }
            StatusCode = httpStatusCode;

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