# encoding: utf-8

module RubydoopExamples
  class IdentityMapper
    def map(key, value, context)
      context.write(key, value)
    end
  end

  class SetupCleanupMapper
    def map(key, value, context)
    end

    def setup(context)
      context.write('setup', 'value')
    end

    def cleanup(context)
      context.write('cleanup', 'value')
    end
  end

  class CountReducer
    def reduce(key, values, context)
      context.write(key, values.count)
    end
  end

  class SetupCleanupReducer
    def reduce(key, values, context)
    end

    def setup(context)
      context.write('setup', 'value')
    end

    def cleanup(context)
      context.write('cleanup', 'value')
    end
  end

  class NumericPartitioner
    def partition(key, value, num_partitions)
      key.to_s.to_i % num_partitions
    end
  end

  class NumericRawComparator
    def compare_raw(bytes1, start1, length1, bytes2, start2, length2)
      String.from_java_bytes(bytes1)[start1, length1].to_i <=> String.from_java_bytes(bytes2)[start2, length2].to_i
    end
  end

  class HardcodedInputFormat
    def splits(context)
      [Java::OrgApacheHadoopMapreduceLibInput::FileSplit.new(Java::OrgApacheHadoopFs::Path.new("ignored"), 0, 0, Java::JavaLang::String[0].new)]
    end

    def create_record_reader(split, context)
      RecordReader.new(split, context)
    end

    class RecordReader
      attr_reader :current_key, :current_value

      def initialize(split, context)
        @closed = false
        @key_values = [%w[key1 val1], %w[key2 val2]]
      end

      def next_key_value
        raise IOError, "closed input" if @closed
        return false if @key_values.empty?
        @current_key, @current_value = @key_values.shift
        true
      end

      def progress
        0.125
      end

      def close
        @closed = true
      end
    end
  end
end
