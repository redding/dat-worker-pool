require 'thread'
require 'dat-worker-pool'
require 'dat-worker-pool/worker'

class DatWorkerPool

  class DefaultWorker
    include DatWorkerPool::Worker

    attr_accessor :on_work

    def initialize(runner, queue)
      @runner  = runner
      @queue   = queue
      @on_work = proc{ |work_item| }

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
      run_callback 'on_start'
      loop do
        break if @shutdown
        fetch_and_do_work
      end
    rescue ShutdownError
    ensure
      run_callback 'on_shutdown'
      @runner.despawn_worker(self)
      @thread = nil
    end

    # * Rescue `ShutdownError` but re-raise it after calling the error
    #   callbacks. This ensures it causes the work loop to exit (see
    #   `work_loop`).
    def fetch_and_do_work
      @runner.increment_worker_waiting
      run_callback 'on_sleep'
      work_item = @queue.pop
      run_callback 'on_wakeup'
      @runner.decrement_worker_waiting
      do_work(work_item) if work_item
    rescue ShutdownError => exception
      handle_exception(exception, work_item)
      raise exception
    rescue StandardError => exception
      handle_exception(exception, work_item)
    end

    def do_work(work_item)
      run_callback 'before_work', work_item
      @on_work.call(work_item)
      run_callback 'after_work', work_item
    end

    def handle_exception(exception, work_item = nil)
      run_callback 'on_error', exception, work_item
    end

  end

end
