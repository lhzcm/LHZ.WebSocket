using System;
using System.IO;
using System.Net;
using System.Text;

namespace LHZ.WebSocket.Http
{
    /// <summary>
    /// Raised when a peer's HTTP handshake message is malformed or exceeds the size limits.
    /// Carries the status code a server should answer with before dropping the connection.
    /// </summary>
    internal sealed class HttpParseException : InvalidOperationException
    {
        public HttpStatusCode StatusCode { get; }

        public HttpParseException(HttpStatusCode statusCode, string message) : base(message)
        {
            StatusCode = statusCode;
        }
    }

    /// <summary>
    /// Line-level reading shared by <see cref="HttpRequest"/> and <see cref="HttpResponse"/>,
    /// with hard size limits so a peer cannot make the handshake buffer without bound.
    /// </summary>
    internal static class HttpParser
    {
        /// <summary>Longest accepted request/status line or header line, in bytes.</summary>
        public const int MaxLineLength = 8192;

        /// <summary>Largest accepted number of header lines.</summary>
        public const int MaxHeaderCount = 100;

        /// <summary>
        /// Reads one CRLF-terminated line, without the terminator. Returns whatever was read
        /// when the stream ends first.
        /// </summary>
        public static string ReadLine(Stream stream)
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
                if (sb.Length >= MaxLineLength)
                {
                    throw new HttpParseException(HttpStatusCode.RequestHeaderFieldsTooLarge,
                        $"HTTP line exceeds {MaxLineLength} bytes");
                }
                sb.Append((char)cur);
                prev = cur;
            }
            return sb.ToString();
        }

        /// <summary>Throws once more than <see cref="MaxHeaderCount"/> header lines have been read.</summary>
        public static void CheckHeaderCount(int headerCount)
        {
            if (headerCount > MaxHeaderCount)
            {
                throw new HttpParseException(HttpStatusCode.RequestHeaderFieldsTooLarge,
                    $"More than {MaxHeaderCount} HTTP headers");
            }
        }
    }
}
