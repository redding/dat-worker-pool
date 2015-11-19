require 'set'
require 'system_timer'
require 'dat-worker-pool'
require 'dat-worker-pool/locked_object'

class DatWorkerPool

  class Runner

    attr_reader :num_workers, :worker_class, :worker_params
    attr_reader :logger_proxy, :queue

    def initialize(args)
      @num_workers   = args[:num_workers]
      @queue         = args[:queue]
      @worker_class  = args[:worker_class]
      @worker_params = args[:worker_params]

      @logger_proxy = if args[:logger]
        LoggerProxy.new(args[:logger])
      else
        NullLoggerProxy.new
      end

      @workers           = LockedArray.new
      @available_workers = LockedSet.new
    end

    def workers
      @workers.values
    end

    def start
      log{ "Starting worker pool with #{@num_workers} worker(s)" }
      @queue.dwp_start
      @num_workers.times.each{ |n| build_worker(n + 1) }
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
      log do
        timeout_message = timeout ? "#{timeout} second(s)" : "none"
        "Shutting down worker pool (timeout: #{timeout_message})"
      end
      begin
        @workers.with_lock{ |m, ws| ws.each(&:dwp_signal_shutdown) }
        @queue.dwp_signal_shutdown
        OptionalTimeout.new(timeout) do
          @queue.dwp_shutdown
          wait_for_workers_to_shutdown
        end
      rescue StandardError => exception
        force_workers_to_shutdown(exception, timeout, backtrace)
        raise exception
      rescue TimeoutInterruptError => exception
        force_workers_to_shutdown(exception, timeout, backtrace)
      end
      log{ "Finished shutting down" }
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

    def log(&message_block)
      @logger_proxy.runner_log(&message_block)
    end

    def worker_log(worker, &message_block)
      @logger_proxy.worker_log(worker, &message_block)
    end

    private

    def build_worker(number)
      @workers.push(@worker_class.new(self, @queue, number).tap(&:dwp_start))
    end

    # use an until loop instead of each to join all the workers, while we are
    # joining a worker a different worker can shutdown and remove itself from
    # the `@workers` array; rescue when joining the workers, ruby will raise any
    # exceptions that aren't handled by a thread when its joined, this allows
    # all the workers to be joined
    def wait_for_workers_to_shutdown
      log{ "Waiting for #{@workers.size} workers to shutdown" }
      while !(worker = @workers.first).nil?
        begin
          worker.dwp_join
        rescue StandardError => exception
          log{ "An error occurred while waiting for worker " \
               "to shutdown ##{worker.dwp_number}" }
        end
        remove_worker(worker)
        log{ "Worker ##{worker.dwp_number} shutdown" }
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
      log{ "Forcing #{@workers.size} workers to shutdown" }
      error = build_forced_shutdown_error(orig_exception, timeout, backtrace)
      while !(worker = @workers.first).nil?
        worker.dwp_raise(error)
        begin
          worker.dwp_join
        rescue StandardError => exception
          log{ "An error occurred while waiting for worker " \
               "to shutdown ##{worker.dwp_number} (forced)" }
        rescue ShutdownError
          # these are expected (because we raised them in the thread) so they
          # don't need to be logged
        end
        remove_worker(worker)
        log{ "Worker ##{worker.dwp_number} shutdown (forced)" }
      end
    end

    # make sure the worker has been removed from the available workers, in case
    # it errored before it was able to make itself unavailable
    def remove_worker(worker)
      self.make_worker_unavailable(worker)
      @workers.delete(worker)
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

    # this needs to be an `Interrupt` to be sure we don't accidentally catch it
    # when rescueing exceptions; in the shutdown methods we rescue any errors
    # from `worker.join`, this will also rescue the timeout error if its a
    # standard error and will keep it from doing a forced shutdown
    TimeoutInterruptError = Class.new(Interrupt)

    module OptionalTimeout
      def self.new(seconds, &block)
        if seconds
          SystemTimer.timeout(seconds, TimeoutInterruptError, &block)
        else
          block.call
        end
      end
    end

    class LoggerProxy < Struct.new(:logger)
      def runner_log(&message_block)
        self.logger.debug("[DWP] #{message_block.call}")
      end
      def worker_log(worker, &message_block)
        self.logger.debug("[DWP-#{worker.dwp_number}] #{message_block.call}")
      end
    end

    class NullLoggerProxy
      def runner_log(&block); end
      def worker_log(worker, &block); end
    end

  end

end
