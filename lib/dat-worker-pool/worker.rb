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

      private

      def run_callback(callback, *args)
        (self.class.send("#{callback}_callbacks") || []).each do |callback|
          callback.call(self, *args)
        end
      end

    end

  end

end
