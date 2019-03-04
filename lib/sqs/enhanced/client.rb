require "set"
require "concurrent"
require "sqs/enhanced/errors"
require "aws-sdk-sqs"

module SQS
  module Enhanced
    class Client
      MAX_BATCH_SIZE = 10
      MAX_PAYLOAD_SIZE = 256 * 1024

      def initialize(client: nil, threads: 32, flush_retry_count: 5, **options)
        if client
          @client = client
        else
          @client = Aws::SQS::Client.new(options)
        end

        @send_message_buffer = Concurrent::Hash.new { |h, k| h[k] = Concurrent::Array.new }
        @current_buffer_size = Concurrent::Hash.new { |h, k| h[k] = Concurrent::AtomicFixnum.new }
        @thread_pool = Concurrent::FixedThreadPool.new(threads)
        @flush_retry_count = flush_retry_count
        @errors = Concurrent::Array.new
        @waiting_futures = Concurrent::Set.new
        @flush_mutex = Mutex.new
      end

      # Add a message to buffer.
      # If count of buffered messages exceed 10 or sum of message size exceeds 256KB,
      # send payload to SQS asynchronously.
      def send_message_buffered(queue_url:, **params)
        add_message_to_buffer(queue_url, params)

        if need_flush?(queue_url)
          flush_async
        end
      end

      def flush_async(queue_url: nil)
        if queue_url
          queue_urls = [queue_url]
        else
          queue_urls = @send_message_buffer.keys
        end

        queue_urls.each do |url|
          buffer = @send_message_buffer[url]
          until buffer.empty? do
            sending = []
            sending_message_size = 0
            @flush_mutex.synchronize do
              until buffer.empty? do
                size = calculate_message_size(buffer[0])
                break if sending_message_size + size > MAX_PAYLOAD_SIZE
                sending << buffer.shift
                sending_message_size += size
              end
              @current_buffer_size[url].decrement(sending_message_size)
            end

            future = Concurrent::Promises.future_on(@thread_pool, sending) do |entries|
              entries.each_with_index do |params, idx|
                params.delete(:queue_url)
                params[:id] = idx.to_s
                params
              end
              send_message_batch_with_retry(queue_url: url, entries: entries)
            end
            @waiting_futures << future
            future.on_resolution_using(@thread_pool) do |fulfilled, value, reason|
              @waiting_futures.delete(future)
              @errors << reason if reason
            end
          end
        end
      end

      def wait_flushing(timeout = nil)
        zipped = Concurrent::Promises.zip_futures_on(@thread_pool, *@waiting_futures)
        exceptions = zipped.reason(timeout)
        raise SQS::Enhanced::SendMessageErrors.new(exceptions) if exceptions
      end

      def flush
        flush_async
        wait_flushing
      end

      private

      def send_message_batch_with_retry(queue_url:, entries:, retry_count: 0)
        resp = @client.send_message_batch(queue_url: queue_url, entries: entries)

        unless resp.failed.empty?
          raise SendMessageBatchFailure.new(resp.failed) if retry_count >= @flush_retry_count

          failed_entries = resp.failed.map { |f| entries[f.id.to_i] }
          send_message_batch_with_retry(queue_url: queue_url, entries: failed_entries, retry_count: retry_count + 1)
        end
      end

      def add_message_to_buffer(queue_url, params)
        @send_message_buffer[queue_url] << params
        @current_buffer_size[queue_url].increment(calculate_message_size(params))
      end

      def need_flush?(queue_url)
        (@send_message_buffer[queue_url].length >= 10) ||
          (@current_buffer_size[queue_url].value >= MAX_PAYLOAD_SIZE)
      end

      def calculate_message_size(params)
        sum = params[:message_body].bytesize
        params[:message_attributes]&.each do |n, a|
          sum += n.bytesize
          sum += a[:data_type].bytesize
          sum += a[:string_value].bytesyze
          sum += a[:binary_value].bytesyze
          sum += a[:string_list_values].sum(&:bytesize)
          sum += a[:binaly_list_values].sum(&:bytesize)
        end
        sum
      end
    end
  end
end
