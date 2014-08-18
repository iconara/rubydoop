# encoding: utf-8

require 'spec_helper'

describe 'proxies' do
  describe RubydoopExamples::IdentityMapper do
    it_behaves_like 'a mapper' do
      let :inputs do
        [%w[key1 val1], %w[key1 val2], %w[key2 val3]]
      end

      it 'writes the entries as is' do
        proxy.run(context)
        expect(outputs).to include(%w[key1 val1])
        expect(outputs).to include(%w[key1 val2])
        expect(outputs).to include(%w[key2 val3])
      end
    end
  end

  describe RubydoopExamples::SetupCleanupMapper do
    it_behaves_like 'a mapper' do
      it 'writes a value pair from #setup' do
        proxy.run(context)
        expect(outputs).to include(%w[setup value])
      end

      it 'writes a value pair from #cleanup' do
        proxy.run(context)
        expect(outputs).to include(%w[cleanup value])
      end
    end
  end

  describe RubydoopExamples::CountReducer do
    ['a reducer', 'a combiner'].each do |role|
      it_behaves_like role do
        let :inputs do
          [%w[key1 val1], %w[key1 val2], %w[key2 val3]]
        end

        it 'writes the entry counts' do
          proxy.run(context)
          expect(outputs).to include(%w[key1 2])
          expect(outputs).to include(%w[key2 1])
        end
      end
    end
  end

  describe RubydoopExamples::SetupCleanupReducer do
    ['a reducer', 'a combiner'].each do |role|
      it_behaves_like role do
        it 'writes a value pair from #setup' do
          proxy.run(context)
          expect(outputs).to include(%w[setup value])
        end

        it 'writes a value pair from #cleanup' do
          proxy.run(context)
          expect(outputs).to include(%w[cleanup value])
        end
      end
    end
  end

  describe RubydoopExamples::NumericPartitioner do
    it_behaves_like 'a partitioner' do
      it 'returns entry modulo number of partitions' do
        partition = proxy.get_partition('123', 'value', 12)
        expect(partition).to eq 3
      end
    end
  end

  describe RubydoopExamples::NumericRawComparator do
    ['a sort comparator', 'a grouping comparator'].each do |role|
      it_behaves_like role do
        it 'returns a number indicating order' do
          result = proxy.compare('111'.to_java_bytes, 0, 2, '999'.to_java_bytes, 2, 1)
          expect(result).to be > 0
        end
      end
    end
  end

  describe RubydoopExamples::HardcodedInputFormat do
    it_behaves_like 'an input format' do
      it 'returns yields one hardcoded split' do
        splits = proxy.get_splits(task_attempt_context)
        expect(splits.size).to eq 1
      end

      context 'a returned record reader' do
        let :reader do
          proxy.create_record_reader(nil, task_attempt_context)
        end

        it 'yields the hardcoded inputs' do
          reader = proxy.create_record_reader(nil, task_attempt_context)
          expect(reader.next_key_value).to eq true
          expect(reader.current_key).to eq 'key1'
          expect(reader.current_value).to eq 'val1'
          expect(reader.next_key_value).to eq true
          expect(reader.current_key).to eq 'key2'
          expect(reader.current_value).to eq 'val2'
          expect(reader.next_key_value).to eq false
        end

        it 'reports the hardcoded progress' do
          expect(reader.progress).to eq 0.125
        end

        it 'can be closed' do
          reader.close
          expect { reader.next_key_value }.to raise_error
        end
      end
    end
  end
end
