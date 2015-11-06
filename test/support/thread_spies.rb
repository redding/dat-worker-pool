require 'thread'

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
