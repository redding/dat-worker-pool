require 'set'
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

      @available_workers = AvailableWorkers.new
    end

    def start
      @queue.start
      @num_workers.times{ build_worker }
    end

    # the workers should be told to shutdown before the queue because the queue
    # shutdown will wake them up; a worker popping on a shutdown queue will
    # always get `nil` back and will loop as fast as allowed until its shutdown
    # flag is flipped, so shutting down the workers then the queue keeps them
    # from looping as fast as possible; if any kind of standard error or the
    # expected timeout error (assuming the workers take too long to shutdown) is
    # raised, force a shutdown; this ensures we shutdown as best as possible
    # instead of letting ruby kill the threads when the process exits;
    # non-timeout errors will be re-raised so they can be caught and handled (or
    # shown when the process exits)
    def shutdown(timeout = nil, backtrace = nil)
      begin
        @workers.each(&:dwp_signal_shutdown)
        @queue.signal_shutdown
        OptionalTimeout.new(timeout) do
          @queue.shutdown
          wait_for_workers_to_shutdown
        end
      rescue StandardError => exception
        force_workers_to_shutdown(exception, timeout, backtrace)
        raise exception
      rescue TimeoutInterruptError => exception
        force_workers_to_shutdown(exception, timeout, backtrace)
      end
    end

    def add_worker(worker)
      @mutex.synchronize{ @workers.push(worker) }
      self.make_worker_available(worker)
    end

    def remove_worker(worker)
      self.make_worker_unavailable(worker)
      @mutex.synchronize{ @workers.delete(worker) }
    end

    def available_worker_count
      @available_workers.size
    end

    def worker_available?
      self.available_worker_count > 0
    end

    def make_worker_available(worker)
      @available_workers.add(worker.object_id)
    end

    def make_worker_unavailable(worker)
      @available_workers.remove(worker.object_id)
    end

    private

    def build_worker
      @worker_class.new(self, @queue).tap(&:dwp_start)
    end

    # use an until loop instead of each to join all the workers, while we are
    # joining a worker a different worker can shutdown and remove itself from
    # the `@workers` array; rescue when joining the workers, ruby will raise any
    # exceptions that aren't handled by a thread when its joined, this allows
    # all the workers to be joined
    def wait_for_workers_to_shutdown
      until @workers.empty?
        worker = @workers.first
        begin
          worker.dwp_join
        rescue StandardError
          self.remove_worker(worker)
        end
      end
    end

    # use an until loop instead of each to join all the workers, while we are
    # joining a worker a different worker can shutdown and remove itself from
    # the `@workers` array; rescue when joining the workers, ruby will raise any
    # exceptions that aren't handled by a thread when its joined, this ensures
    # if the hard shutdown is raised and not rescued (for example, in the
    # workers ensure), then it won't cause the forced shutdown to end
    # prematurely
    def force_workers_to_shutdown(orig_exception, timeout, backtrace)
      error = build_forced_shutdown_error(orig_exception, timeout, backtrace)
      until @workers.empty?
        worker = @workers.first
        worker.dwp_raise(error)
        begin
          worker.dwp_join
        rescue StandardError, ShutdownError
        end
        self.remove_worker(worker)
      end
    end

    def build_forced_shutdown_error(orig_exception, timeout, backtrace)
      if orig_exception.kind_of?(TimeoutInterruptError)
        ShutdownError.new("Timed out shutting down (#{timeout} seconds).").tap do |e|
          e.set_backtrace(backtrace) if backtrace
        end
      else
        ShutdownError.new("Errored while shutting down: #{orig_exception.inspect}").tap do |e|
          e.set_backtrace(orig_exception.backtrace)
        end
      end
    end

    module OptionalTimeout
      def self.new(seconds, &block)
        if seconds
          SystemTimer.timeout(seconds, TimeoutInterruptError, &block)
        else
          block.call
        end
      end
    end

    # this needs to be an `Interrupt` to be sure we don't accidentally catch it
    # when rescueing exceptions; in the shutdown methods we rescue any errors
    # from `worker.join`, this will also rescue the timeout error if its a
    # standard error and will keep it from doing a forced shutdown
    TimeoutInterruptError = Class.new(Interrupt)

    class AvailableWorkers
      def initialize
        @mutex = Mutex.new
        @set   = Set.new
      end

      def get
        @mutex.synchronize{ @set }
      end

      def size
        @mutex.synchronize{ @set.size }
      end

      def add(value)
        @mutex.synchronize{ @set.add(value) }
      end

      def remove(value)
        @mutex.synchronize{ @set.delete(value) }
      end
    end

  end

end
