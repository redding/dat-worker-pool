require 'thread'
require 'dat-worker-pool'

class DatWorkerPool

  class Worker

    attr_accessor :on_work, :on_error_callbacks
    attr_accessor :on_start_callbacks, :on_shutdown_callbacks
    attr_accessor :on_sleep_callbacks, :on_wakeup_callbacks
    attr_accessor :before_work_callbacks, :after_work_callbacks

    def initialize(queue)
      @queue = queue
      @on_work = proc{ |worker, work_item| }
      @on_error_callbacks    = []
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

    def shutdown
      @shutdown = true
    end

    def running?
      !!(@thread && @thread.alive?)
    end

    def join(*args)
      @thread.join(*args) if running?
    end

    def raise(*args)
      @thread.raise(*args) if running?
    end

    private

    def work_loop
      @on_start_callbacks.each{ |p| p.call(self) }
      loop do
        break if @shutdown
        fetch_and_do_work
      end
    ensure
      @on_shutdown_callbacks.each{ |p| p.call(self) }
      @thread = nil
    end

    def fetch_and_do_work
      @on_sleep_callbacks.each{ |p| p.call(self) }
      work_item = @queue.pop
      @on_wakeup_callbacks.each{ |p| p.call(self) }
      do_work(work_item) if work_item
    rescue ShutdownError => exception
      handle_exception(exception, work_item)
      raise exception
    rescue StandardError => exception
      handle_exception(exception, work_item)
    end

    def do_work(work_item)
      @before_work_callbacks.each{ |p| p.call(self, work_item) }
      @on_work.call(self, work_item)
      @after_work_callbacks.each{ |p| p.call(self, work_item) }
    end

    def handle_exception(exception, work_item = nil)
      @on_error_callbacks.each{ |p| p.call(self, exception, work_item) }
    end

  end

end
