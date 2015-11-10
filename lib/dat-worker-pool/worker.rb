require 'thread'

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
        @runner  = runner
        @queue   = queue
        @running = false
        @thread  = nil
      end

      def work(work_item)
        run_callback('before_work', work_item)
        work!(work_item)
        run_callback('after_work', work_item)
      end

      def start
        @running = true
        @thread ||= Thread.new{ work_loop }
      end

      def shutdown
        @running = false
      end

      def running?
        !!@running
      end

      def shutdown?
        !self.running?
      end

      # this is needed because even if the running flag has been set to false
      # (meaning the worker has been shutdown) the thread may still be alive
      # because its `work` is taking a long time or its still trying to shut
      # down
      def thread_alive?
        !!(@thread && @thread.alive?)
      end

      def join(*args)
        @thread.join(*args) if self.thread_alive?
      end

      def raise(*args)
        @thread.raise(*args) if self.thread_alive?
      end

      private

      # overwrite this method to add custom work logic; this has to be
      # overwritten or the workers will not know how to handle a work item
      def work!(work_item)
        raise NotImplementedError
      end

      # rescue `ShutdownError` and ignore it, its already been passed to the
      # on-error callbacks in `fetch_and_work`; the shutdown error is just used
      # to force the worker to exit its infinite loop if the worker pool is
      # forcing itself to shutdown
      def work_loop
        run_callback 'on_start'
        fetch_and_work while self.running?
      rescue ShutdownError
      ensure
        run_callback 'on_shutdown'
        @runner.despawn_worker(self)
        @thread = nil
      end

      # rescue `ShutdownError` but re-raise it after calling the on-error
      # callbacks, this ensures it causes the loop in `work_loop` to exit
      def fetch_and_work
        @runner.increment_worker_waiting
        run_callback 'on_sleep'
        work_item = @queue.pop
        run_callback 'on_wakeup'
        @runner.decrement_worker_waiting
        work(work_item) if work_item
      rescue ShutdownError => exception
        handle_exception(exception, work_item)
        Thread.current.raise exception
      rescue StandardError => exception
        handle_exception(exception, work_item)
      end

      def handle_exception(exception, work_item = nil)
        run_callback('on_error', exception, work_item)
      end

      def run_callback(callback, *args)
        (self.class.send("#{callback}_callbacks") || []).each do |callback|
          callback.call(self, *args)
        end
      end

    end

  end

end
