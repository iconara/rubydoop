# encoding: utf-8

require 'spec_helper'

shared_examples 'proxy-unit-tests' do
  let :target do
    double(:target)
  end

  class RubyClass
  end

  let :ruby_class do
    RubyClass
  end

  before do
    allow(ruby_class).to receive(:new).and_return(target)
  end

  it 'forwards #setup' do
    expect(target).to receive(:setup).with(context)
    proxy.setup(context)
  end

  it 'wraps #setup errors in runtime exceptions' do
    error = IOError.new('fake')
    expect(target).to receive(:setup).and_raise(error)
    expect { proxy_with_exceptions { proxy.setup(context) } }.to raise_error(error)
  end

  it 'does not call #setup if it does not exist' do
    proxy.setup(context)
  end

  it 'forwards main method' do
    proxy.setup(context)
    expect(target).to receive(main_method).with(:key, [], context)
    proxy.send(main_method, :key, [], context)
  end

  it 'wraps main method errors in runtime exceptions' do
    error = IOError.new('fake')
    proxy.setup(context)
    expect(target).to receive(main_method).and_raise(error)
    expect { proxy_with_exceptions { proxy.send(main_method, :key, [], context) } }.to raise_error(error)
  end

  it 'forwards #cleanup' do
    proxy.setup(context)
    expect(target).to receive(:cleanup).with(context)
    proxy.cleanup(context)
  end

  it 'wraps #cleanup errors in runtime exceptions' do
    error = IOError.new('fake')
    proxy.setup(context)
    expect(target).to receive(:cleanup).and_raise(error)
    expect { proxy_with_exceptions { proxy.cleanup(context) } }.to raise_error(error)
  end

  it 'does not call #cleanup if it does not exist' do
    proxy.setup(context)
    proxy.cleanup(context)
  end
end

describe Java::Rubydoop::MapperProxy do
  include_context 'mapper-proxy'
  include_context 'proxy-unit-tests'

  let :main_method do
    :map
  end
end

describe Java::Rubydoop::ReducerProxy do
  include_context 'reducer-proxy'
  include_context 'proxy-unit-tests'

  let :main_method do
    :reduce
  end
end

describe Java::Rubydoop::CombinerProxy do
  include_context 'combiner-proxy'
  include_context 'proxy-unit-tests'

  let :main_method do
    :reduce
  end
end


