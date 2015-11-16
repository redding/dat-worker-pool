require 'thread'
require 'dat-worker-pool/locked_object'
require 'dat-worker-pool/queue'

class DatWorkerPool

  class DefaultQueue
    include DatWorkerPool::Queue

    attr_reader :on_push_callbacks, :on_pop_callbacks

    def initialize
      @work_items = LockedArray.new
      @cond_var   = ConditionVariable.new

      @on_push_callbacks = []
      @on_pop_callbacks  = []
    end

    def work_items; @work_items.values; end
    def empty?;     @work_items.empty?; end

    def on_push(&block); @on_push_callbacks << block; end
    def on_pop(&block);  @on_pop_callbacks  << block; end

    private

    # wake up workers (`@cond_var.broadcast`) who are sleeping because of `pop`
    def shutdown!
      @work_items.with_lock{ @cond_var.broadcast }
    end

    # add the work item and wakeup (`@cond_var.signal`) the first sleeping
    # worker (from calling `pop`)
    def push!(work_item)
      @work_items.with_lock do |mutex, work_items|
        work_items << work_item
        @cond_var.signal
      end
      @on_push_callbacks.each{ |p| p.call(self, work_item) }
    end

    # check if the queue is empty, if so sleep (`@cond_var.wait(@mutex)`) until
    # signaled (via `push!` or `shutdown!`); once a work item is available pop
    # it from the front and return it; if shutdown, return `nil` which the
    # workers will ignore
    def pop!
      work_item = @work_items.with_lock do |mutex, work_items|
        while !self.shutdown? && work_items.empty?
          @cond_var.wait(mutex)
        end
        work_items.shift unless self.shutdown?
      end
      @on_pop_callbacks.each{ |p| p.call(self, work_item) } if !work_item.nil?
      work_item
    end

  end

end
