class DatWorkerPool

  module Queue

    def self.included(klass)
      klass.class_eval do
        include InstanceMethods
      end
    end

    module InstanceMethods

      def dwp_start
        @dwp_running = true
        start!
      end

      def dwp_signal_shutdown
        @dwp_running = false
      end

      def dwp_shutdown
        self.dwp_signal_shutdown
        shutdown!
      end

      def running?
        !!@dwp_running
      end

      def shutdown?
        !self.running?
      end

      def dwp_push(*args)
        raise "Unable to add work when shut down" if self.shutdown?
        push!(*args)
      end

      def dwp_pop
        return if self.shutdown?
        pop!
      end

      private

      # overwrite this method to add custom start logic; this is a no-op by
      # default because we don't require a queue to have custom start logic
      def start!; end

      # overwrite this method to add custom shutdown logic; this is a no-op by
      # default because we don't require a queue to have custom shutdown logic;
      # more than likely you will want to use this to "wakeup" worker threads
      # that are sleeping waiting to pop work from the queue (see the default
      # queue for an example using mutexes and condition variables)
      def shutdown!; end

      # overwrite this method to add custom push logic; this doesn't have to be
      # overwritten but if it isn't, you will not be able to add work items using
      # the queue (and the `add_work` method on `DatWorkerPool` will not work);
      # more than likely this should add work to the queue and "signal" the
      # workers so they know to process it (see the default queue for an example
      # using mutexes and condition variables)
      def push!(*args)
        raise NotImplementedError
      end

      # overwrite this method to add custom pop logic; this has to be overwritten
      # or the workers will not be able to get work that needs to be processed;
      # this is intended to sleep the worker threads (see the default queue for an
      # example using mutexes and condition variables); if this returns `nil` the
      # workers will ignore it and go back to sleep, `nil` is not a valid work
      # item to process; also check if the queue is shutdown when waking up
      # workers, you probably don't want to hand-off work while everything is
      # shutting down
      def pop!
        raise NotImplementedError
      end

    end

  end

end
