require "sqrewdriver/version"
require "sqrewdriver/serdes/json_serde"

module Sqrewdriver
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

require "sqrewdriver/client"
