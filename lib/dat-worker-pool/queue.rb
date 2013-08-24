require 'thread'

class DatWorkerPool

  class Queue

    def initialize
      @work_items = []
      @shutdown = false
      @mutex = Mutex.new
      @condition_variable = ConditionVariable.new
    end

    def work_items
      @mutex.synchronize{ @work_items }
    end

    # Add the work_item and wake up the first worker (the `signal`) that's
    # waiting (because of `wait_for_work_item`)
    def push(work_item)
      raise "Unable to add work while shutting down" if @shutdown
      @mutex.synchronize do
        @work_items << work_item
        @condition_variable.signal
      end
    end

    def pop
      return if @shutdown
      @mutex.synchronize do
        @condition_variable.wait(@mutex) while !@shutdown && @work_items.empty?
        @work_items.shift
      end
    end

    def empty?
      @mutex.synchronize{ @work_items.empty? }
    end

    # wake up any workers who are idle (because of `wait_for_work_item`)
    def shutdown
      @shutdown = true
      @mutex.synchronize{ @condition_variable.broadcast }
    end

  end

end
