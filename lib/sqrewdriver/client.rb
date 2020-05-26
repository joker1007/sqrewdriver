require "set"
require "monitor"
require "concurrent"
require "sqrewdriver/errors"
require "aws-sdk-sqs"
require "thread"

module Sqrewdriver
  class Client
    MAX_BATCH_SIZE = 10
    MAX_PAYLOAD_SIZE = 256 * 1024

    def initialize(queue_url:, client: nil, threads: 32, buffer_size: nil, serializer: Sqrewdriver.default_serializer, aggregate_messages_per: nil, flush_retry_count: 5, **options)
      if client
        @client = client
      else
        @client = Aws::SQS::Client.new(options)
      end

      @queue_url = queue_url
      @buffer_size = buffer_size
      if @buffer_size && @buffer_size > 0
        @message_buffer = SizedQueue.new(@buffer_size)
      else
        @message_buffer = Queue.new
      end
      @thread_pool = Concurrent::FixedThreadPool.new(threads)
      @waiting_futures = Concurrent::Set.new
      @errors = Concurrent::Array.new
      @flush_mutex = Mutex.new
      @aggregate_messages_per = aggregate_messages_per

      ensure_serializer_for_aggregation!(serializer)

      @sending_buffer = SendingBuffer.new(client: @client, queue_url: queue_url, serializer: serializer, thread_pool: @thread_pool, flush_retry_count: flush_retry_count)
    end

    # Add a message to buffer.
    #
    # If count of buffered messages exceed 10 or aggregate_messages_per
    # else if sum of message size exceeds 256KB,
    # send payload to SQS asynchronously.
    def send_message_buffered(message)
      add_message_to_buffer(message)

      if need_flush?
        flush_async
      end
    end

    class SendingBuffer
      class Chunk
        attr_reader :data, :bytesize

        def initialize(flush_retry_count: 5)
          @data = []
          @bytesize = 0
          @flush_retry_count = flush_retry_count
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

      def initialize(client:, queue_url:, serializer:, thread_pool:, flush_retry_count:)
        super()
        @client = client
        @queue_url = queue_url
        @chunks = Concurrent::Array.new
        @serializer = serializer
        @thread_pool = thread_pool
        @flush_retry_count = flush_retry_count
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
          if @chunks.last.size == MAX_BATCH_SIZE || @chunks.last.bytesize + add_size > MAX_PAYLOAD_SIZE
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
          sending = synchronize { chunks.shift }
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
        begin
          resp = @client.send_message_batch(queue_url: @queue_url, entries: entries)
        rescue => e
          raise SendMessageBatchRequestError.new(e) if retry_count >= @flush_retry_count

          send_message_batch_with_retry(entries: entries, retry_count: retry_count + 1)
        end

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
          pop_multi_messages_from_buffer(@aggregate_messages_per) do |messages|
            @sending_buffer.add_aggregated_messages(messages)
          end
        else
          pop_message_from_buffer do |message|
            @sending_buffer.add_message(message)
          end
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

      @errors.concat(zipped.reason) if zipped.reason
      raise Sqrewdriver::SendMessageErrors.new(@errors) unless @errors.empty?
    end

    def clear_errors
      @errors.clear
    end

    def flush(timeout = nil)
      flush_async
      wait_flushing(timeout)
    end

    private

    def add_message_to_buffer(message)
      @message_buffer << message
    end

    def pop_message_from_buffer
      message = @message_buffer.shift(true)
      if block_given?
        yield message
      end
      message
    rescue ThreadError
      nil
    end

    def pop_multi_messages_from_buffer(count)
      messages = []
      count.times do
        begin
          messages << @message_buffer.shift(true)
        rescue ThreadError
          break
        end
      end
      if block_given?
        yield messages
      end
      messages
    end

    def need_flush?
      if @buffer_size && @message_buffer.length >= @buffer_size
        return true
      else
        @message_buffer.length >= (@aggregate_messages_per&.*(10) || 10)
      end
    end

    def send_first_chunk_async
      future = @sending_buffer.send_first_chunk_async
      @waiting_futures << future
      future.on_resolution_using(@thread_pool) do |fulfilled, value, reason|
        if reason
          @errors << reason
        end
        @waiting_futures.delete(future)
      end
    end

    def ensure_serializer_for_aggregation!(serializer)
      valid_serializer = @aggregate_messages_per.nil? || serializer.is_a?(Sqrewdriver::Serdes::JSONSerde)
      unless valid_serializer
        raise InvalidSerializer, "If you use `aggregate_messages_per`, serializer must be `Sqrewdriver::Serdes::JSONSerde`"
      end
    end
  end
end
