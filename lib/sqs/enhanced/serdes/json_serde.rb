module SQS
  module Enhanced
    module Serdes
      class JSONSerde
        class JSONWrapper
          def dump(val, **options)
            JSON.generate(val, **options)
          end

          def load(val, **options)
            JSON.load(val, nil, **options)
          end
        end

        def initialize(backend: :json, serialize_options: {}, deserialize_options: {})
          @serialize_options = serialize_options
          @deserialize_options = deserialize_options
          @backend = lookup_json_backend(backend)
        end

        def dump(val)
          @backend.dump(val, **@serialize_options)
        end

        def load(val)
          @backend.load(val, **@deserialize_options)
        end

        private

        def lookup_json_backend(name)
          case name
          when :json
            require "json"
            JSONWrapper.new
          when :oj
            require "oj"
            Oj
          end
        end
      end
    end
  end
end
