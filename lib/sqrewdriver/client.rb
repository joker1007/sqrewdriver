require "set"
require "monitor"
require "concurrent"
require "sqrewdriver/errors"
require "aws-sdk-sqs"

module Sqrewdriver
  class Client
    MAX_BATCH_SIZE = 10
    MAX_PAYLOAD_SIZE = 256 * 1024

    def initialize(queue_url:, client: nil, threads: 32, serializer: Sqrewdriver.default_serializer, aggregate_messages_per: nil, flush_retry_count: 5, **options)
      if client
        @client = client
      else
        @client = Aws::SQS::Client.new(options)
      end

      @queue_url = queue_url
      @message_buffer = Concurrent::Array.new
      @thread_pool = Concurrent::FixedThreadPool.new(threads)
      @flush_retry_count = flush_retry_count
      @waiting_futures = Concurrent::Set.new
      @flush_mutex = Mutex.new
      @aggregate_messages_per = aggregate_messages_per

      ensure_serializer_for_aggregation!(serializer)

      @sending_buffer = SendingBuffer.new(client: @client, queue_url: queue_url, serializer: serializer, thread_pool: @thread_pool)
    end

    # Add a message to buffer.
    #
    # If count of buffered messages exceed 10 or aggregate_messages_per
    # else if sum of message size exceeds 256KB,
    # send payload to SQS asynchronously.
    def send_message_buffered(queue_url: nil, **params)
      add_message_to_buffer(params)

      if need_flush?
        flush_async
      end
    end

    class SendingBuffer
      class Chunk
        attr_reader :data, :bytesize

        def initialize
          @data = []
          @bytesize = 0
        end

        def add(message, size)
          @data << message
          @bytesize += size
        end

        def size
          @data.size
        end
      end

      include MonitorMixin

      attr_reader :chunks

      def initialize(client:, queue_url:, serializer:, thread_pool:)
        super()
        @client = client
        @queue_url = queue_url
        @chunks = Concurrent::Array.new
        @serializer = serializer
        @thread_pool = thread_pool
      end

      def add_message(message)
        serialized = @serializer.dump(message[:message_body])
        message[:message_body] = serialized
        add_size = calculate_message_size(serialized, message[:attributes])

        synchronize do
          @chunks << Chunk.new if @chunks.empty?
          if @chunks.last.size == MAX_BATCH_SIZE || @chunks.last.bytesize + add_size > MAX_PAYLOAD_SIZE
            new_chunk = Chunk.new
            new_chunk.add(message, add_size)
            @chunks << new_chunk
          else
            @chunks.last.add(message, add_size)
          end
        end
      end

      def add_aggregated_messages(messages)
        base_message = messages[0]

        message_bodies = messages.map { |m| m[:message_body] }
        serialized = @serializer.dump(message_bodies)
        base_message[:message_body] = serialized
        add_size = calculate_message_size(serialized, base_message[:message_attributes])

        synchronize do
          @chunks << Chunk.new if @chunks.empty?
          if @chunks.last.bytesize + add_size > MAX_PAYLOAD_SIZE
            new_chunk = Chunk.new
            new_chunk.add(base_message, add_size)
            @chunks << new_chunk
          else
            @chunks.last.add(base_message, add_size)
          end
        end
      end

      def has_full_chunk?
        @chunks.size > 1
      end

      def send_first_chunk_async
        Concurrent::Promises.future_on(@thread_pool, @chunks) do |chunks|
          sending = chunks.shift
          if sending
            sending.data.each_with_index do |params, idx|
              params[:id] = idx.to_s
              params
            end
            send_message_batch_with_retry(entries: sending.data)
          end
        end
      end

      private

      def calculate_message_size(body, attributes)
        sum = body.bytesize
        attributes&.each do |n, a|
          sum += n.bytesize
          sum += a[:data_type].bytesize
          sum += a[:string_value].bytesyze
          sum += a[:binary_value].bytesyze
          sum += a[:string_list_values].sum(&:bytesize)
          sum += a[:binaly_list_values].sum(&:bytesize)
        end
        sum
      end

      def send_message_batch_with_retry(entries:, retry_count: 0)
        resp = @client.send_message_batch(queue_url: @queue_url, entries: entries)

        unless resp.failed.empty?
          raise SendMessageBatchFailure.new(resp.failed) if retry_count >= @flush_retry_count

          failed_entries = resp.failed.map { |f| entries[f.id.to_i] }
          send_message_batch_with_retry(entries: failed_entries, retry_count: retry_count + 1)
        end
      end
    end

    def flush_async
      until @message_buffer.empty? do
        if @aggregate_messages_per
          messages = @message_buffer.shift(@aggregate_messages_per)
          @sending_buffer.add_aggregated_messages(messages)
        else
          message = @message_buffer.shift
          @sending_buffer.add_message(message)
        end

        if @sending_buffer.has_full_chunk?
          send_first_chunk_async
        end
      end

      send_first_chunk_async
    end

    def wait_flushing(timeout = nil)
      zipped = Concurrent::Promises.zip_futures_on(@thread_pool, *@waiting_futures)
      unless zipped.wait(timeout)
        raise Sqrewdriver::SendMessageTimeout
      end

      exceptions = zipped.reason
      raise Sqrewdriver::SendMessageErrors.new(exceptions) if exceptions
    end

    def flush(timeout = nil)
      flush_async
      wait_flushing(timeout)
    end

    private

    def add_message_to_buffer(params)
      @message_buffer << params
    end

    def need_flush?
      @message_buffer.length >= (@aggregate_messages_per&.*(10) || 10)
    end

    def send_first_chunk_async
      future = @sending_buffer.send_first_chunk_async 
      @waiting_futures << future
      future.on_resolution_using(@thread_pool) do |fulfilled, value, reason|
        @waiting_futures.delete(future)
      end
    end

    def ensure_serializer_for_aggregation!(serializer)
      valid_serializer = @aggregate_messages_per.nil? || serializer.is_a?(Sqrewdriver::Serdes::JSONSerde)
      unless valid_serializer
        raise InvalidSerializer, "If you use `aggregate_messages_per`, serializer must be `Oj` or `MultiJson`"
      end
    end
  end
end
