require 'thread'
require 'dat-worker-pool'

class DatWorkerPool

  class DefaultWorker

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

    # * Rescue `ShutdownError` but don't do anything with it. We want to handle
    #   the error but we just want it to cause the worker to exit its work loop.
    #   If the `ShutdownError` isn't rescued, it will be raised when the worker
    #   is joined.
    def work_loop
      @on_start_callbacks.each{ |p| p.call(self) }
      loop do
        break if @shutdown
        fetch_and_do_work
      end
    rescue ShutdownError
    ensure
      @on_shutdown_callbacks.each{ |p| p.call(self) }
      @thread = nil
    end

    # * Rescue `ShutdownError` but re-raise it after calling the error
    #   callbacks. This ensures it causes the work loop to exit (see
    #   `work_loop`).
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
