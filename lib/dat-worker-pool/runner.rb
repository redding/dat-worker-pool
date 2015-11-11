require 'system_timer'
require 'thread'
require 'dat-worker-pool'

class DatWorkerPool

  class Runner

    attr_reader :num_workers, :worker_class, :worker_params
    attr_reader :queue, :workers

    def initialize(args)
      @num_workers   = args[:num_workers]
      @queue         = args[:queue]
      @worker_class  = args[:worker_class]
      @worker_params = args[:worker_params]

      @mutex   = Mutex.new
      @workers = []

      @workers_waiting = WorkersWaiting.new
    end

    def start
      @queue.start
      @num_workers.times{ spawn_worker }
    end

    # the workers should be told to shutdown before the queue because the queue
    # shutdown will wake them up; a worker popping on a shutdown queue will
    # always get `nil` back and will loop as fast as allowed until its shutdown
    # flag is flipped, so shutting down the workers then the queue keeps them from
    # looping as fast as possible; use an until loop instead of each to join all
    # the workers, while we are joining a worker a different worker can shutdown
    # and remove itself from the `@workers` array
    def shutdown(timeout = nil)
      begin
        OptionalTimeout.new(timeout) do
          @workers.each(&:shutdown)
          @queue.shutdown
          @workers.first.join until @workers.empty?
        end
      rescue TimeoutError
        force_shutdown(timeout, caller)
      end
    end

    def workers_waiting_count
      @workers_waiting.count
    end

    def increment_worker_waiting
      @workers_waiting.increment
    end

    def decrement_worker_waiting
      @workers_waiting.decrement
    end

    def despawn_worker(worker)
      @mutex.synchronize{ @workers.delete(worker) }
    end

    private

    def spawn_worker
      @mutex.synchronize do
        @worker_class.new(self, @queue).tap do |w|
          @workers << w
          w.start
        end
      end
    end

    # use an until loop instead of each to join all the workers, while we are
    # joining a worker a different worker can shutdown and remove itself from
    # the `@workers` array; `rescue false` when joining the workers, ruby will
    # raise any exceptions that aren't handled by a thread when its joined, this
    # ensures if the hard shutdown is raised and not rescued (for example, in
    # the workers ensure), then it won't cause the forced shutdown to end
    # prematurely
    def force_shutdown(timeout, backtrace)
      error = ShutdownError.new("Timed out shutting down (#{timeout} seconds).")
      error.set_backtrace(backtrace)
      until @workers.empty?
        worker = @workers.first
        worker.raise(error)
        worker.join rescue false
        @workers.delete(worker)
      end
    end

    module OptionalTimeout
      def self.new(seconds, &block)
        if seconds
          SystemTimer.timeout(seconds, TimeoutError, &block)
        else
          block.call
        end
      end
    end

    class WorkersWaiting
      attr_reader :count

      def initialize
        @mutex = Mutex.new
        @count = 0
      end

      def increment
        @mutex.synchronize{ @count += 1 }
      end

      def decrement
        @mutex.synchronize{ @count -= 1 }
      end
    end

  end

end
