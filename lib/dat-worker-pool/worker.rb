require 'thread'

class DatWorkerPool

  class Worker

    def initialize(pool, queue, workers_waiting, &block)
      @pool            = pool
      @queue           = queue
      @workers_waiting = workers_waiting
      @block           = block
      @shutdown        = false
      @thread          = Thread.new{ work_loop }
    end

    def shutdown
      @shutdown = true
    end

    def join(*args)
      @thread.join(*args) if @thread
    end

    protected

    def work_loop
      loop do
        @workers_waiting.increment
        work_item = @queue.pop
        @workers_waiting.decrement
        !@shutdown ? @block.call(work_item) : break
      end
    ensure
      @pool.despawn_worker(self)
    end

  end

end
