module SQS
  module Enhanced
    class SendMessageErrors < StandardError
      attr :errors

      def initialize(errors)
        @errors = errors
      end

      def messages
        @errors.map(&:message)
      end
    end

    class SendMessageBatchFailure < StandardError
      attr_reader :failed

      def initialize(failed)
        @failed = failed
      end
    end
  end
end
