module SQS
  module Enhanced
    module Serdes
      module RawSerde
        class << self
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
end
