require "sqs/enhanced/version"

module SQS
  module Enhanced
    class << self
      # @return [#dump]
      def default_serializer
        @default_serializer
      end

      # @param serializer [#dump]
      def default_serializer=(serializer)
        @default_serializer = serializer
      end
    end

    begin
      require "oj"
      self.default_serializer = Oj
    rescue LoadError
      self.default_serializer = Serdes::RawSerde
    end
  end
end

require "sqs/enhanced/client"
