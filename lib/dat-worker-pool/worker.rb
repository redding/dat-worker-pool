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
        self.wait_for_work
        break if @shutdown
        @block.call @queue.pop
      end
    ensure
      @pool.despawn_worker(self)
    end

    # Wait for work to process by checking if the queue is empty.
    def wait_for_work
      while @queue.empty?
        return if @shutdown
        @workers_waiting.increment
        @queue.wait_for_work_item
        @workers_waiting.decrement
      end
    end

  end

end
