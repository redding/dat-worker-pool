require 'thread'

class ThreadSpy
  attr_reader :join_seconds, :join_called
  attr_reader :raised_exception

  def initialize(&block)
    @block    = block
    @mutex    = Mutex.new
    @cond_var = ConditionVariable.new

    @join_seconds     = nil
    @join_called      = false
    @raised_exception = nil

    @thread = Thread.new do
      @mutex.synchronize{ @cond_var.wait(@mutex) } if @block.nil?
      @block.call
    end
  end

  def block=(new_block)
    @block = new_block
    @mutex.synchronize{ @cond_var.signal }
  end

  def join(seconds = nil)
    @join_seconds = seconds
    @join_called  = true
    @thread.join(seconds)
  end

  def raise(exception)
    @raised_exception = exception
    @thread.raise(exception)
  end

  def status; @thread.status; end
  def alive?; @thread.alive?; end
end

class MutexSpy < Mutex
  attr_accessor :synchronize_called

  def initialize
    @synchronize_called = false
    super
  end

  def synchronize
    @synchronize_called = true
    super
  end
end

class ConditionVariableSpy < ConditionVariable
  attr_reader :signal_called, :broadcast_called
  attr_reader :wait_called_on, :wait_call_count

  def initialize
    @signal_called    = false
    @broadcast_called = false
    @wait_called_on   = nil
    @wait_call_count  = 0
    super
  end

  def signal
    @signal_called = true
    super
  end

  def broadcast
    @broadcast_called = true
    super
  end

  def wait(mutex)
    @wait_called_on  = mutex
    @wait_call_count += 1
    super
  end
end
