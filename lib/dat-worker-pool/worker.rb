require 'thread'

class DatWorkerPool

  class Worker
    attr_writer :on_work, :on_waiting, :on_continuing, :on_shutdown

    def initialize(queue)
      @queue = queue
      @on_work       = proc{ |work_item| }
      @on_waiting    = proc{ |worker| }
      @on_continuing = proc{ |worker| }
      @on_shutdown   = proc{ |worker| }

      @shutdown = false
      @thread   = nil
    end

    def start
      @thread ||= Thread.new{ work_loop }
    end

    def running?
      @thread && @thread.alive?
    end

    def shutdown
      @shutdown = true
      @queue.shutdown
    end

    def join(*args)
      @thread.join(*args) if running?
    end

    protected

    def work_loop
      loop do
        @on_waiting.call(self)
        work_item = @queue.pop
        @on_continuing.call(self)
        !@shutdown ? @on_work.call(work_item) : break
      end
    ensure
      @on_shutdown.call(self)
      @thread = nil
    end

  end

end
