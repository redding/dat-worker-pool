require 'thread'

class DatWorkerPool

  class DefaultQueue

    attr_accessor :on_push_callbacks, :on_pop_callbacks

    def initialize
      @work_items         = []
      @shutdown           = false
      @mutex              = Mutex.new
      @condition_variable = ConditionVariable.new

      @on_pop_callbacks  = []
      @on_push_callbacks = []
    end

    def start
      @shutdown = false
    end

    # * Wakes up any threads (`@condition_variable.broadcast`) who are sleeping
    #   because of `pop`.
    def shutdown
      @shutdown = true
      @mutex.synchronize{ @condition_variable.broadcast }
    end

    # * Add the work and wake up the first thread waiting from calling `pop`
    #  (`@condition_variable.signal`).
    def push(work_item)
      raise "Unable to add work while shutting down" if @shutdown
      @mutex.synchronize do
        @work_items << work_item
        @condition_variable.signal
      end
      @on_push_callbacks.each(&:call)
    end

    # * Sleeps the current thread (`@condition_variable.wait(@mutex)`) until it
    #   is signaled via `push` or `shutdown`.
    def pop
      return if @shutdown
      item = @mutex.synchronize do
        @condition_variable.wait(@mutex) while !@shutdown && @work_items.empty?
        @work_items.shift
      end
      @on_pop_callbacks.each(&:call)
      item
    end

    def work_items
      @mutex.synchronize{ @work_items }
    end

    def empty?
      @mutex.synchronize{ @work_items.empty? }
    end

    def shutdown?
      @shutdown
    end

  end

end
