using System;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Text;
using System.Text.Json;

namespace PipeReaderUtility
{
    public static class PipeReaderExtension
    {
        private readonly static byte[] _comma = Encoding.UTF8.GetBytes(",");

        private readonly static byte[] _open = Encoding.UTF8.GetBytes("[");

        private readonly static byte[] _close = Encoding.UTF8.GetBytes("]");

        private readonly static byte[] _objectStart = Encoding.UTF8.GetBytes("{");

        public async static IAsyncEnumerable<T> ChunkProcessMessagesAsync<T>(PipeReader pipeReader, JsonSerializerOptions jsonSerializerOptions)
        {
            var readRes = await pipeReader.ReadAsync();
            do
            {
                if (TryParseMessages<T>(in readRes, jsonSerializerOptions, out var item, out var commited, out var examined))
                {
                    yield return item;
                }

                pipeReader.AdvanceTo(commited, examined);
                if (readRes.IsCompleted || readRes.IsCanceled)
                {
                    break;
                }

                readRes = await pipeReader.ReadAsync();
            } while (true);
        }


        public static bool TryParseMessages<T>(in ReadResult read, JsonSerializerOptions jsonSerializerOptions, out T item, out SequencePosition commited, out SequencePosition examined)
        {
            item = default;
            var buffer = read.Buffer;
            commited = buffer.Start;
            examined = buffer.Start;
            if (buffer.FirstSpan.StartsWith(_comma))
            {
                commited = buffer.GetPosition(_comma.Length);
                examined = commited;
                return false;
            }
            else if (buffer.FirstSpan.StartsWith(_open))
            {
                commited = buffer.GetPosition(_open.Length);
                examined = commited;
                return false;
            }
            else if (buffer.FirstSpan.StartsWith(_close))
            {
                commited = buffer.GetPosition(_close.Length);
                examined = commited;
                return false;
            }
            else if (buffer.IsEmpty)
            {
                return false;
            }
            else if (!buffer.FirstSpan.StartsWith(_objectStart))
            {
                throw new NotSupportedException("should start with , [ ] {");
            }

            try
            {
                var jsonReader = new Utf8JsonReader(buffer);
                jsonReader.Read();
                item = JsonSerializer.Deserialize<T>(ref jsonReader, jsonSerializerOptions);
                commited = buffer.GetPosition(jsonReader.BytesConsumed);
                examined = commited;
            }
            catch
            {
                examined = buffer.End;
                return false;
            }

            return true;
        }
    }
}
