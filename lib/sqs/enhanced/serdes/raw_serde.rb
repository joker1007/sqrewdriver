module SQS
  module Enhanced
    module Serdes
      class RawSerde
        def initialize(serialize_options: {}, deserialize_options: {})
        end

        def dump(val)
          val
        end

        def load(val)
          val
        end
      end
    end
  end
end
