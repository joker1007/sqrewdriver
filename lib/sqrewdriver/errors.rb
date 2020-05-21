module Sqrewdriver
  class SendMessageErrors < StandardError
    attr :errors

    def initialize(errors)
      @errors = Array(errors).compact
    end

    def messages
      @errors.map(&:message)
    end

    def to_s
      errors.map(&:inspect).join(", ")
    end
  end

  class SendMessageBatchFailure < StandardError
    attr_reader :failed

    def initialize(failed)
      @failed = failed
    end
  end

  class SendMessageTimeout < StandardError; end
end
