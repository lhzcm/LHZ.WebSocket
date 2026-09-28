using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;

namespace LHZ.WebSocket.Http
{
    /// <summary>
    /// Builds and parses an HTTP response: the status line plus all headers.
    /// </summary>
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
        /// <summary>Creates a response to send.</summary>
        /// <param name="statusCode">The HTTP status code.</param>
        /// <param name="httpVersion">HTTP version string, e.g. HTTP/1.1.</param>
        /// <param name="headers">Response headers; a new empty collection is used when null.</param>
        public HttpResponse(HttpStatusCode statusCode, string httpVersion, System.Net.Http.Headers.HttpHeaders? headers = null)
        {
            StatusCode = statusCode;
            HttpVersion = httpVersion;
            Headers = headers ?? new HttpHeaders();
        }
        /// <summary>Reads and parses a response from the stream.</summary>
        /// <param name="stream">The stream to read from.</param>
        /// <returns>The parsed response.</returns>
        public static HttpResponse GetRequestFromStream(Stream stream)
        {
            var httpResponse = new HttpResponse();
            httpResponse.Parse(stream);
            return httpResponse;
        }
        /// <summary>Serializes the status line and headers to the stream and flushes it.</summary>
        /// <param name="stream">The destination stream.</param>
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

            HttpVersion = parts[0];
            // Parse the numeric code only. Enum.TryParse would also accept status *names* and
            // would reject any valid code that the BCL enum happens not to define.
            if (!int.TryParse(parts[1], System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out int statusCode) ||
                statusCode < 100 || statusCode > 599)
            {
                throw new InvalidOperationException($"Invalid HTTP Status Code: {parts[1]}");
            }
            StatusCode = (HttpStatusCode)statusCode;

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
                    catch (Exception)
                    {
                        // Some headers (e.g. Connection, Keep-Alive) are restricted in
                        // System.Net.Http.Headers; fall back to storing them unvalidated.
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