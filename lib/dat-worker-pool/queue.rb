require 'thread'

class DatWorkerPool

  class Queue

    attr_accessor :on_push_callbacks, :on_pop_callbacks

    def initialize
      @work_items = []
      @shutdown = false
      @mutex = Mutex.new
      @condition_variable = ConditionVariable.new

      @on_pop_callbacks  = []
      @on_push_callbacks = []
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
      @on_push_callbacks.each(&:call)
    end

    def pop
      return if @shutdown
      item = @mutex.synchronize do
        @condition_variable.wait(@mutex) while !@shutdown && @work_items.empty?
        @work_items.shift
      end
      @on_pop_callbacks.each(&:call)
      item
    end

    def empty?
      @mutex.synchronize{ @work_items.empty? }
    end

    def start
      @shutdown = false
    end

    # wake up any workers who are idle (because of `wait_for_work_item`)
    def shutdown
      @shutdown = true
      @mutex.synchronize{ @condition_variable.broadcast }
    end

    def shutdown?
      @shutdown
    end

  end

end
