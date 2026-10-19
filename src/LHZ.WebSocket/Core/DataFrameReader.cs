using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace LHZ.WebSocket.Core
{
    /// <summary>
    /// Reads and parses WebSocket data frames from a <see cref="Stream"/>.
    /// Supports both synchronous and asynchronous enumeration.
    /// </summary>
    public class DataFrameReader
    {
        private readonly Stream _stream;

        /// <summary>Creates a reader over the given stream.</summary>
        /// <param name="stream">The stream to read frames from.</param>
        public DataFrameReader(Stream stream)
        {
            _stream = stream;
        }

        /// <summary>Yields data frames synchronously until a close frame is encountered.</summary>
        public IEnumerable<DataFrame> Read()
        {
            while (true)
            {
                var frame = ReadFrame();
                yield return frame;
                if (frame.Opcode == Enums.OpCode.Close)
                {
                    yield break;
                }
            }
        }

        /// <summary>
        /// Yields data frames asynchronously until a close frame is encountered
        /// or cancellation is requested.
        /// </summary>
        public async IAsyncEnumerable<DataFrame> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var frame = await ReadFrameAsync(cancellationToken);
                yield return frame;
                if (frame.Opcode == Enums.OpCode.Close)
                {
                    yield break;
                }
            }
        }

        /// <summary>
        /// Reads a single WebSocket frame synchronously.
        /// Parses the header (FIN, opcode, mask, payload length) then reads the payload.
        /// </summary>
        private DataFrame ReadFrame()
        {
            Span<byte> header = stackalloc byte[2];
            StreamReadExact(header);
            int payloadLength = (byte)(header[1] & 0x7F);
            bool masked = (header[1] & 0x80) == 0x80;
            UInt32 maskingKey = 0;
            int headerLength = 2;
            // Extended payload length: 2 bytes for 126, 8 bytes for 127
            if (payloadLength == 126)
            {
                Span<byte> lengthBytes = stackalloc byte[2];
                StreamReadExact(lengthBytes);
                headerLength += 2;
                payloadLength = (((int)lengthBytes[0]) << 8) | lengthBytes[1];
            }
            else if (payloadLength == 127)
            {
                Span<byte> lengthBytes = stackalloc byte[8];
                StreamReadExact(lengthBytes);
                if (lengthBytes[0] != 0 || lengthBytes[1] != 0 || lengthBytes[2] != 0 || lengthBytes[3] != 0)
                {
                    throw new Exception($"Single frame data not larger than {int.MaxValue} bytes");
                }
                headerLength += 8;
                payloadLength = (((int)lengthBytes[4]) << 24) | (((int)lengthBytes[5]) << 16) | (((int)lengthBytes[6]) << 8) | lengthBytes[7];
            }

            if (masked)
            {
                Span<byte> maskingKeyArray = stackalloc byte[4];
                StreamReadExact(maskingKeyArray);
                maskingKey = DataFrame.MaskingKeyToUint32(ref maskingKeyArray);
                headerLength += 4;
            }
            var buffer = ArrayPool<byte>.Shared.Rent(payloadLength + headerLength);
            //Fill payload in one read to avoid multiple reads from the stream.
            StreamReadExact(buffer, headerLength, payloadLength);

            return DataFrame.CreateDataFrame(buffer, payloadLength + headerLength, headerLength, header[0], maskingKey, masked);
        }

        /// <summary>
        /// Reads a single WebSocket frame asynchronously.
        /// </summary>
        private async Task<DataFrame> ReadFrameAsync(CancellationToken cancellationToken = default)
        {
            // The longest header is 14 bytes: 2 base + 8 extended length + 4 masking key.
            // stackalloc cannot survive an await, so the header is read into a pooled buffer.
            byte[] headerBuffer = ArrayPool<byte>.Shared.Rent(14);
            byte[]? frameBuffer = null;
            try
            {
                // Base header: 2 bytes.
                int headerBytesRead = 0;
                while (headerBytesRead < 2)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    int bytesRead = await _stream.ReadAsync(headerBuffer.AsMemory(headerBytesRead, 2 - headerBytesRead), cancellationToken).ConfigureAwait(false);
                    if (bytesRead == 0)
                    {
                        throw new EndOfStreamException("Stream closed while reading data frame.");
                    }
                    headerBytesRead += bytesRead;
                }

                int payloadLength = (byte)(headerBuffer[1] & 0x7F);
                bool masked = (headerBuffer[1] & 0x80) == 0x80;
                int headerLength = 2;
                // Extended payload length: 2 bytes for 126, 8 bytes for 127
                if (payloadLength == 126)
                {
                    headerLength += 2;
                }
                else if (payloadLength == 127)
                {
                    headerLength += 8;
                }
                if (masked)
                {
                    headerLength += 4;
                }

                // The rest of the header (extended length and masking key) is contiguous,
                // so it is read together instead of with one await per field.
                while (headerBytesRead < headerLength)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    int bytesRead = await _stream.ReadAsync(headerBuffer.AsMemory(headerBytesRead, headerLength - headerBytesRead), cancellationToken).ConfigureAwait(false);
                    if (bytesRead == 0)
                    {
                        throw new EndOfStreamException("Stream closed while reading data frame.");
                    }
                    headerBytesRead += bytesRead;
                }

                if (payloadLength == 126)
                {
                    payloadLength = (((int)headerBuffer[2]) << 8) | headerBuffer[3];
                }
                else if (payloadLength == 127)
                {
                    if (headerBuffer[2] != 0 || headerBuffer[3] != 0 || headerBuffer[4] != 0 || headerBuffer[5] != 0)
                    {
                        throw new Exception($"Single frame data not larger than {int.MaxValue} bytes");
                    }
                    payloadLength = (((int)headerBuffer[6]) << 24) | (((int)headerBuffer[7]) << 16) | (((int)headerBuffer[8]) << 8) | headerBuffer[9];
                }

                UInt32 maskingKey = 0;
                if (masked)
                {
                    Span<byte> maskingKeyArray = headerBuffer.AsSpan(headerLength - 4, 4);
                    maskingKey = DataFrame.MaskingKeyToUint32(ref maskingKeyArray);
                }

                // One pooled allocation for the whole frame, as in ReadFrame.
                frameBuffer = ArrayPool<byte>.Shared.Rent(payloadLength + headerLength);
                int payloadBytesRead = 0;
                while (payloadBytesRead < payloadLength)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    int bytesRead = await _stream.ReadAsync(frameBuffer.AsMemory(headerLength + payloadBytesRead, payloadLength - payloadBytesRead), cancellationToken).ConfigureAwait(false);
                    if (bytesRead == 0)
                    {
                        throw new EndOfStreamException("Stream closed while reading data frame.");
                    }
                    payloadBytesRead += bytesRead;
                }

                var frame = DataFrame.CreateDataFrame(frameBuffer, payloadLength + headerLength, headerLength, headerBuffer[0], maskingKey, masked);
                // The frame owns the buffer now; do not return it in the finally block.
                frameBuffer = null;
                return frame;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(headerBuffer);
                if (frameBuffer != null)
                {
                    ArrayPool<byte>.Shared.Return(frameBuffer);
                }
            }
        }

        /// <summary>Reads exactly <paramref name="buffer"/> buffer from the stream (sync).</summary>
        private void StreamReadExact(Span<byte> buffer)
        {
            int bytesReadTotal = 0;
            while (bytesReadTotal < buffer.Length)
            {
                int bytesRead = _stream.Read(buffer.Slice(bytesReadTotal));
                if (bytesRead == 0)
                    throw new EndOfStreamException("Stream closed while reading data frame.");
                bytesReadTotal += bytesRead;
            }
        }
        /// <summary>Reads exactly <paramref name="buffer"/> buffer from the stream (sync).</summary>
        private void StreamReadExact(byte[] buffer, int offset, int count)
        {
            int bytesReadTotal = 0;
            while (bytesReadTotal < count)
            {
                int bytesRead = _stream.Read(buffer, offset + bytesReadTotal, count - bytesReadTotal);
                if (bytesRead == 0)
                    throw new EndOfStreamException("Stream closed while reading data frame.");
                bytesReadTotal += bytesRead;
            }
        }

        /// <summary>Reads exactly <paramref name="count"/> bytes from the stream (async).</summary>
        private async Task<byte[]> StreamReadExactAsync(int count, byte[] buffer, CancellationToken cancellationToken = default)
        {
            if(buffer == null || buffer.Length < count)
            {
                buffer = new byte[count];
            }
            int bytesReadTotal = 0;
            while (bytesReadTotal < count)
            {
                // Throw rather than return a partially filled buffer: a short read would be
                // parsed as a valid frame by the caller.
                cancellationToken.ThrowIfCancellationRequested();
                int bytesRead = await _stream.ReadAsync(buffer, bytesReadTotal, count - bytesReadTotal, cancellationToken);
                if (bytesRead == 0)
                    throw new EndOfStreamException("Stream closed while reading data frame.");
                bytesReadTotal += bytesRead;
            }
            return buffer;
        }
    }
}
