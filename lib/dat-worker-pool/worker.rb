require 'thread'

class DatWorkerPool

  class Worker

    attr_accessor :on_work
    attr_accessor :on_start_callbacks, :on_shutdown_callbacks
    attr_accessor :on_sleep_callbacks, :on_wakeup_callbacks
    attr_accessor :before_work_callbacks, :after_work_callbacks

    def initialize(queue)
      @queue = queue
      @on_work = proc{ |worker, work_item| }
      @on_start_callbacks    = []
      @on_shutdown_callbacks = []
      @on_sleep_callbacks    = []
      @on_wakeup_callbacks   = []
      @before_work_callbacks = []
      @after_work_callbacks  = []

      @shutdown = false
      @thread   = nil
    end

    def start
      @thread ||= Thread.new{ work_loop }
    end

    def running?
      !!(@thread && @thread.alive?)
    end

    def shutdown
      @shutdown = true
    end

    def join(*args)
      @thread.join(*args) if running?
    end

    def raise(*args)
      @thread.raise(*args) if running?
      self.join
    end

    private

    def work_loop
      @on_start_callbacks.each{ |p| p.call(self) }
      loop do
        @on_sleep_callbacks.each{ |p| p.call(self) }
        work_item = @queue.pop
        @on_wakeup_callbacks.each{ |p| p.call(self) }
        break if @shutdown
        do_work(work_item) if work_item
      end
    ensure
      @on_shutdown_callbacks.each{ |p| p.call(self) }
      @thread = nil
    end

    def do_work(work_item)
      @before_work_callbacks.each{ |p| p.call(self, work_item) }
      @on_work.call(self, work_item)
      @after_work_callbacks.each{ |p| p.call(self, work_item) }
    end

  end

end
