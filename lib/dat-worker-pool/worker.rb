require 'thread'
require 'timeout'
require 'dat-worker-pool/runner'

class DatWorkerPool

  module Worker

    # these are standard error classes that we rescue, handle and don't reraise
    # in the work loop, this keeps the worker thread from shutting down
    # unexpectedly; `Timeout::Error` is a common non `StandardError` exception
    # that should be treated like a `StandardError`, we don't want an uncaught
    # `Timeout::Error` to shutdown a worker thread
    STANDARD_ERROR_CLASSES = [StandardError, Timeout::Error].freeze

    def self.included(klass)
      klass.class_eval do
        extend ClassMethods
        include InstanceMethods
      end
    end

    module ClassMethods

      def on_start_callbacks;       @on_start_callbacks       ||= []; end
      def on_shutdown_callbacks;    @on_shutdown_callbacks    ||= []; end
      def on_available_callbacks;   @on_available_callbacks   ||= []; end
      def on_unavailable_callbacks; @on_unavailable_callbacks ||= []; end
      def on_error_callbacks;       @on_error_callbacks       ||= []; end
      def before_work_callbacks;    @before_work_callbacks    ||= []; end
      def after_work_callbacks;     @after_work_callbacks     ||= []; end

      def on_start(&block);       self.on_start_callbacks       << block; end
      def on_shutdown(&block);    self.on_shutdown_callbacks    << block; end
      def on_available(&block);   self.on_available_callbacks   << block; end
      def on_unavailable(&block); self.on_unavailable_callbacks << block; end
      def on_error(&block);       self.on_error_callbacks       << block; end
      def before_work(&block);    self.before_work_callbacks    << block; end
      def after_work(&block);     self.after_work_callbacks     << block; end

      def prepend_on_start(&block);       self.on_start_callbacks.unshift(block);       end
      def prepend_on_shutdown(&block);    self.on_shutdown_callbacks.unshift(block);    end
      def prepend_on_available(&block);   self.on_available_callbacks.unshift(block);   end
      def prepend_on_unavailable(&block); self.on_unavailable_callbacks.unshift(block); end
      def prepend_on_error(&block);       self.on_error_callbacks.unshift(block);       end
      def prepend_before_work(&block);    self.before_work_callbacks.unshift(block);    end
      def prepend_after_work(&block);     self.after_work_callbacks.unshift(block);     end

    end

    module InstanceMethods

      attr_reader :dwp_number

      def initialize(runner, queue, number)
        @dwp_runner, @dwp_queue, @dwp_number = runner, queue, number
        @dwp_running = false
        @dwp_thread  = nil
      end

      def dwp_start
        @dwp_running = true
        @dwp_thread ||= Thread.new{ dwp_work_loop }
      end

      def dwp_signal_shutdown
        @dwp_running = false
      end

      def dwp_running?
        !!@dwp_running
      end

      def dwp_shutdown?
        !self.dwp_running?
      end

      # this is needed because even if the running flag has been set to false
      # (meaning the worker has been shutdown) the thread may still be alive
      # because its `work` is taking a long time or its still trying to shut
      # down
      def dwp_thread_alive?
        !!(@dwp_thread && @dwp_thread.alive?)
      end

      def dwp_join(*args)
        @dwp_thread.join(*args) if self.dwp_thread_alive?
      end

      def dwp_raise(*args)
        @dwp_thread.raise(*args) if self.dwp_thread_alive?
      end

      private

      # Helpers
      def number; @dwp_number;               end
      def params; @dwp_runner.worker_params; end
      def queue;  @dwp_runner.queue;         end

      # overwrite this method to add custom work logic; this has to be
      # overwritten or the workers will not know how to handle a work item
      def work!(work_item)
        raise NotImplementedError
      end

      # rescue `ShutdownError` but re-raise it after calling the on-error
      # callbacks, this ensures it causes the loop to exit
      def dwp_work_loop
        dwp_setup
        while self.dwp_running?
          begin
            if !(work_item = queue.dwp_pop).nil?
              begin
                dwp_make_unavailable
                dwp_work(work_item)
              rescue ShutdownError => exception
                dwp_handle_exception(exception, work_item)
                Thread.current.raise exception
              rescue *STANDARD_ERROR_CLASSES => exception
                dwp_handle_exception(exception, work_item)
              ensure
                dwp_make_available
              end
            end
          rescue *STANDARD_ERROR_CLASSES => exception
            dwp_handle_exception(exception, work_item)
          end
        end
      ensure
        dwp_teardown
      end

      def dwp_setup
        dwp_log{ "Starting" }
        begin
          dwp_run_callback 'on_start'
          dwp_make_available
        rescue *STANDARD_ERROR_CLASSES => exception
          dwp_handle_exception(exception)
          Thread.current.raise exception
        end
      end

      # this is a separate method so the test runner can call it individually
      def dwp_make_unavailable
        @dwp_runner.make_worker_unavailable(self)
        dwp_run_callback 'on_unavailable'
        dwp_log{ "Unavailable" }
      end

      # this is a separate method so the test runner can call it individually
      def dwp_make_available
        @dwp_runner.make_worker_available(self)
        dwp_run_callback 'on_available'
        dwp_log{ "Available" }
      end

      # this is a separate method so the test runner can call it individually
      def dwp_work(work_item)
        dwp_log{ "Working, item: #{work_item.inspect}" }
        dwp_run_callback('before_work', work_item)
        work!(work_item)
        dwp_run_callback('after_work', work_item)
      end

      def dwp_teardown
        begin
          dwp_make_unavailable
          dwp_run_callback 'on_shutdown'
        rescue *STANDARD_ERROR_CLASSES => exception
          dwp_handle_exception(exception)
        end
        dwp_log{ "Shutdown" }
        @dwp_running = false
        @dwp_thread  = nil
      end

      def dwp_handle_exception(exception, work_item = nil)
        begin
          dwp_log_exception(exception)
          dwp_run_callback('on_error', exception, work_item)
        rescue *STANDARD_ERROR_CLASSES => on_error_exception
          # errors while running on-error callbacks are logged but otherwise
          # ignored to keep the worker from crashing, ideally these should be
          # caught by the on-error callbacks themselves and never get here
          dwp_log_exception(on_error_exception)
        end
      end

      def dwp_run_callback(callback, *args)
        (self.class.send("#{callback}_callbacks") || []).each do |callback|
          self.instance_exec(*args, &callback)
        end
      end

      def dwp_log(&message_block)
        @dwp_runner.worker_log(self, &message_block)
      end

      def dwp_log_exception(exception)
        dwp_log{ "#{exception.class}: #{exception.message}" }
        (exception.backtrace || []).each{ |l| dwp_log{ l } }
      end

    end

    module TestHelpers

      def test_runner(worker_class, options = nil)
        TestRunner.new(worker_class, options)
      end

      class TestRunner
        attr_reader :worker_class, :worker
        attr_reader :queue, :dwp_runner

        def initialize(worker_class, options = nil)
          @worker_class = worker_class

          @queue = options[:queue] || begin
            require 'dat-worker-pool/default_queue'
            DatWorkerPool::DefaultQueue.new
          end

          @dwp_runner = DatWorkerPool::Runner.new({
            :num_workers   => MIN_WORKERS,
            :logger        => options[:logger],
            :queue         => @queue,
            :worker_class  => @worker_class,
            :worker_params => options[:params]
          })

          @worker = worker_class.new(@dwp_runner, @queue, 1)
        end

        def run(work_item)
          self.start
          self.make_unavailable
          self.work(work_item)
          self.make_available
          self.shutdown
        end

        def work(work_item)
          self.worker.instance_eval{ dwp_work(work_item) }
        end

        def error(exception, work_item = nil)
          run_callback('on_error', self.worker, exception, work_item)
        end

        def start
          run_callback('on_start', self.worker)
        end

        def shutdown
          run_callback('on_shutdown', self.worker)
        end

        def make_unavailable
          self.worker.instance_eval{ dwp_make_unavailable }
        end

        def make_available
          self.worker.instance_eval{ dwp_make_available }
        end

        private

        def run_callback(callback, worker, *args)
          self.worker.instance_eval{ dwp_run_callback(callback, *args) }
        end
      end

    end

  end

end
