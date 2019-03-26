require "sqs/enhanced/version"
require "sqs/enhanced/serdes/json_serde"

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

    self.default_serializer = Serdes::JSONSerde.new
  end
end

require "sqs/enhanced/client"
