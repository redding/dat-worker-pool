require 'thread'
require 'dat-worker-pool/runner'

class DatWorkerPool

  module Worker

    def self.included(klass)
      klass.class_eval do
        extend ClassMethods
        include InstanceMethods
      end
    end

    module ClassMethods

      def on_start_callbacks;    @on_start_callbacks    ||= []; end
      def on_shutdown_callbacks; @on_shutdown_callbacks ||= []; end
      def on_sleep_callbacks;    @on_sleep_callbacks    ||= []; end
      def on_wakeup_callbacks;   @on_wakeup_callbacks   ||= []; end
      def on_error_callbacks;    @on_error_callbacks    ||= []; end
      def before_work_callbacks; @before_work_callbacks ||= []; end
      def after_work_callbacks;  @after_work_callbacks  ||= []; end

      def on_start(&block);    self.on_start_callbacks    << block; end
      def on_shutdown(&block); self.on_shutdown_callbacks << block; end
      def on_sleep(&block);    self.on_sleep_callbacks    << block; end
      def on_wakeup(&block);   self.on_wakeup_callbacks   << block; end
      def on_error(&block);    self.on_error_callbacks    << block; end
      def before_work(&block); self.before_work_callbacks << block; end
      def after_work(&block);  self.after_work_callbacks  << block; end

      def prepend_on_start(&block);    self.on_start_callbacks.unshift(block);    end
      def prepend_on_shutdown(&block); self.on_shutdown_callbacks.unshift(block); end
      def prepend_on_sleep(&block);    self.on_sleep_callbacks.unshift(block);    end
      def prepend_on_wakeup(&block);   self.on_wakeup_callbacks.unshift(block);   end
      def prepend_on_error(&block);    self.on_error_callbacks.unshift(block);    end
      def prepend_before_work(&block); self.before_work_callbacks.unshift(block); end
      def prepend_after_work(&block);  self.after_work_callbacks.unshift(block);  end

    end

    module InstanceMethods

      def initialize(runner, queue)
        @dwp_runner  = runner
        @dwp_queue   = queue
        @dwp_running = false
        @dwp_thread  = nil
      end

      def work(work_item)
        dwp_run_callback('before_work', work_item)
        work!(work_item)
        dwp_run_callback('after_work', work_item)
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
            work_item = nil
            @dwp_runner.increment_worker_waiting
            dwp_run_callback 'on_sleep'
            work_item = queue.pop
            dwp_run_callback 'on_wakeup'
            @dwp_runner.decrement_worker_waiting
            work(work_item) if !work_item.nil?
          rescue ShutdownError => exception
            dwp_handle_exception(exception, work_item) if work_item
            Thread.current.raise exception
          rescue StandardError => exception
            dwp_handle_exception(exception, work_item)
          end
        end
      ensure
        dwp_teardown
      end

      def dwp_setup
        @dwp_runner.add_worker(self)
        begin
          dwp_run_callback 'on_start'
        rescue StandardError => exception
          dwp_handle_exception(exception)
          Thread.current.raise exception
        end
      end

      def dwp_teardown
        begin
          dwp_run_callback 'on_shutdown'
        rescue StandardError => exception
          dwp_handle_exception(exception)
        end
        @dwp_runner.remove_worker(self)
        @dwp_running = false
        @dwp_thread  = nil
      end

      def dwp_handle_exception(exception, work_item = nil)
        dwp_run_callback('on_error', exception, work_item)
      end

      def dwp_run_callback(callback, *args)
        (self.class.send("#{callback}_callbacks") || []).each do |callback|
          self.instance_exec(*args, &callback)
        end
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
            :queue         => @queue,
            :worker_class  => @worker_class,
            :worker_params => options[:params]
          })

          @worker = worker_class.new(@dwp_runner, @queue)
        end

        def run(work_item)
          self.start
          self.sleep
          self.wakeup
          self.work(work_item)
          self.shutdown
        end

        def work(work_item)
          @worker.work(work_item)
        end

        def error(exception, work_item = nil)
          run_callback('on_error', @worker, exception, work_item)
        end

        def start
          run_callback('on_start', @worker)
        end

        def shutdown
          run_callback('on_shutdown', @worker)
        end

        def sleep
          run_callback('on_sleep', @worker)
        end

        def wakeup
          run_callback('on_wakeup', @worker)
        end

        private

        def run_callback(callback, worker, *args)
          worker.instance_eval{ dwp_run_callback(callback, *args) }
        end
      end

    end

  end

end
