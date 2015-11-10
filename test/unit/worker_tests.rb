require 'assert'
require 'dat-worker-pool/worker'

require 'system_timer'
require 'dat-worker-pool/default_queue'
require 'dat-worker-pool/runner'

module DatWorkerPool::Worker

  class UnitTests < Assert::Context
    desc "DatWorkerPool::Worker"
    setup do
      @worker_class = Class.new do
        include DatWorkerPool::Worker
      end
    end
    subject{ @worker_class }

    should have_imeths :on_start_callbacks, :on_shutdown_callbacks
    should have_imeths :on_sleep_callbacks, :on_wakeup_callbacks
    should have_imeths :on_error_callbacks
    should have_imeths :before_work_callbacks, :after_work_callbacks
    should have_imeths :on_start, :on_shutdown
    should have_imeths :on_sleep, :on_wakeup
    should have_imeths :on_error
    should have_imeths :before_work, :after_work
    should have_imeths :prepend_on_start, :prepend_on_shutdown
    should have_imeths :prepend_on_sleep, :prepend_on_wakeup
    should have_imeths :prepend_on_error
    should have_imeths :prepend_before_work, :prepend_after_work

    should "not have any callbacks by default" do
      assert_equal [], subject.on_start_callbacks
      assert_equal [], subject.on_shutdown_callbacks
      assert_equal [], subject.on_sleep_callbacks
      assert_equal [], subject.on_wakeup_callbacks
      assert_equal [], subject.on_error_callbacks
      assert_equal [], subject.before_work_callbacks
      assert_equal [], subject.after_work_callbacks
    end

    should "allow appending callbacks" do
      callback = proc{ Factory.string }
      # add a callback to each type to show we are appending
      subject.on_start_callbacks    << proc{ Factory.string }
      subject.on_shutdown_callbacks << proc{ Factory.string }
      subject.on_sleep_callbacks    << proc{ Factory.string }
      subject.on_wakeup_callbacks   << proc{ Factory.string }
      subject.on_error_callbacks    << proc{ Factory.string }
      subject.before_work_callbacks << proc{ Factory.string }
      subject.after_work_callbacks  << proc{ Factory.string }

      subject.on_start(&callback)
      assert_equal callback, subject.on_start_callbacks.last

      subject.on_shutdown(&callback)
      assert_equal callback, subject.on_shutdown_callbacks.last

      subject.on_sleep(&callback)
      assert_equal callback, subject.on_sleep_callbacks.last

      subject.on_wakeup(&callback)
      assert_equal callback, subject.on_wakeup_callbacks.last

      subject.on_error(&callback)
      assert_equal callback, subject.on_error_callbacks.last

      subject.before_work(&callback)
      assert_equal callback, subject.before_work_callbacks.last

      subject.after_work(&callback)
      assert_equal callback, subject.after_work_callbacks.last
    end

    should "allow prepending callbacks" do
      callback = proc{ Factory.string }
      # add a callback to each type to show we are appending
      subject.on_start_callbacks    << proc{ Factory.string }
      subject.on_shutdown_callbacks << proc{ Factory.string }
      subject.on_sleep_callbacks    << proc{ Factory.string }
      subject.on_wakeup_callbacks   << proc{ Factory.string }
      subject.on_error_callbacks    << proc{ Factory.string }
      subject.before_work_callbacks << proc{ Factory.string }
      subject.after_work_callbacks  << proc{ Factory.string }

      subject.prepend_on_start(&callback)
      assert_equal callback, subject.on_start_callbacks.first

      subject.prepend_on_shutdown(&callback)
      assert_equal callback, subject.on_shutdown_callbacks.first

      subject.prepend_on_sleep(&callback)
      assert_equal callback, subject.on_sleep_callbacks.first

      subject.prepend_on_wakeup(&callback)
      assert_equal callback, subject.on_wakeup_callbacks.first

      subject.prepend_on_error(&callback)
      assert_equal callback, subject.on_error_callbacks.first

      subject.prepend_before_work(&callback)
      assert_equal callback, subject.before_work_callbacks.first

      subject.prepend_after_work(&callback)
      assert_equal callback, subject.after_work_callbacks.first
    end

    def shutdown_worker_queue_and_wait_for_thread_to_stop
      @worker.shutdown if @worker
      @queue.shutdown
      wait_for_worker_thread_to_stop
    end

    def wait_for_worker_thread_to_stop
      SystemTimer.timeout(1){ @worker.join } if @worker
    end

  end

  class InitTests < UnitTests
    desc "when init"
    setup do
      @queue  = DatWorkerPool::DefaultQueue.new.tap(&:start)
      @runner = DatWorkerPool::Runner.new(:queue => @queue)

      @worker = TestWorker.new(@runner, @queue)
    end
    teardown do
      shutdown_worker_queue_and_wait_for_thread_to_stop
    end
    subject{ @worker }

    should have_imeths :work
    should have_imeths :start, :shutdown, :running?, :shutdown?
    should have_imeths :thread_alive?, :join, :raise
    should have_imeths :queue, :params

    should "know its queue and params" do
      assert_equal @queue,                subject.queue
      assert_equal @runner.worker_params, subject.params
    end

    should "call `work!` and its before/after work callbacks using `work`" do
      work_item = Factory.string
      subject.work(work_item)

      assert_same work_item, subject.before_work_item_worked_on
      assert_same work_item, subject.item_worked_on
      assert_same work_item, subject.after_work_item_worked_on
    end

    should "start a thread when its started" do
      thread = subject.start

      assert_instance_of Thread, thread
      assert_true thread.alive?
    end

    should "know if its running and if its thread is alive or not" do
      assert_false subject.running?
      assert_false subject.thread_alive?

      subject.start
      assert_true subject.running?
      assert_true subject.thread_alive?

      subject.shutdown
      assert_false subject.running?
      assert_true subject.thread_alive?

      shutdown_worker_queue_and_wait_for_thread_to_stop
      assert_false subject.running?
      assert_false subject.thread_alive?
    end

    should "despawn itself from its runner when its thread stops" do
      despawned_worker = nil
      Assert.stub(@runner, :despawn_worker){ |worker| despawned_worker = worker }
      subject.start

      shutdown_worker_queue_and_wait_for_thread_to_stop
      assert_same subject, despawned_worker
    end

    should "allow joining and raising on its thread" do
      thread_spy = ThreadSpy.new
      Assert.stub(Thread, :new){ thread_spy }

      subject.start

      subject.join
      assert_nil thread_spy.join_seconds
      assert_true thread_spy.join_called

      seconds = Factory.integer
      subject.join(seconds)
      assert_equal seconds, thread_spy.join_seconds

      exception = Factory.exception
      subject.raise(exception)
      assert_equal exception, thread_spy.raised_exception
    end

    should "not allow joining or raising if it hasn't been started" do
      assert_nothing_raised{ subject.join(Factory.integer) }
      assert_nothing_raised{ subject.raise(Factory.exception) }
    end

    should "increment its runners workers waiting everytime it sleeps" do
      increment_call_count = 0
      Assert.stub(@runner, :increment_worker_waiting){ increment_call_count += 1 }

      # the worker immediately goes to sleep because there isn't any work
      subject.start
      assert_equal 1, increment_call_count

      # the worker wakes up and runs this work item, then goes back to sleep
      @queue.push(Factory.string)
      assert_equal 2, increment_call_count
    end

    should "decrement its runners workers waiting everytime it wakes up" do
      decrement_call_count = 0
      Assert.stub(@runner, :decrement_worker_waiting){ decrement_call_count += 1 }

      # the worker immediately goes to sleep because there isn't any work
      subject.start
      assert_equal 0, decrement_call_count

      # the worker wakes up and runs this work item
      @queue.push(Factory.string)
      assert_equal 1, decrement_call_count

      # the worker wakes up when it and its queue are shut down
      shutdown_worker_queue_and_wait_for_thread_to_stop
      assert_equal 2, decrement_call_count
    end

    should "call its `work` method on any pushed items while running" do
      assert_nil subject.item_worked_on
      subject.start
      assert_nil subject.item_worked_on

      work_item = Factory.string
      @queue.push(work_item)
      assert_equal work_item, subject.item_worked_on
    end

    should "not call its `work` method if it pops a `nil` work item" do
      subject.start

      @queue.push(nil)
      assert_false subject.work_called

      # when the queue is shutdown it returns `nil`, so we shouldn't call `work`
      # when shutting down
      shutdown_worker_queue_and_wait_for_thread_to_stop
      assert_false subject.work_called
    end

    should "not stop its thread when an error occurs while running" do
      exception = Factory.exception
      setup_work_loop_to_raise_exception(exception)
      subject.start

      @queue.push(Factory.string)
      assert_true subject.thread_alive?
    end

    should "run its on-error callbacks if an error occurs while running" do
      exception = Factory.exception
      error_method = setup_work_loop_to_raise_exception(exception)
      subject.start
      work_item = Factory.string
      @queue.push(work_item)

      assert_equal exception, subject.on_error_exception
      # the on-sleep callback will not include a work item if it errors
      exp = error_method == :on_sleep_error ? nil : work_item
      assert_equal exp, subject.on_error_work_item
    end

    should "stop its thread when a shutdown error is raised while running" do
      exception = Factory.exception(DatWorkerPool::ShutdownError)
      setup_work_loop_to_raise_exception(exception)
      subject.start
      @queue.push(Factory.string)

      wait_for_worker_thread_to_stop
      assert_false subject.thread_alive?
    end

    should "not re-raise a shutdown error when joined" do
      exception = Factory.exception(DatWorkerPool::ShutdownError)
      setup_work_loop_to_raise_exception(exception)
      subject.start
      @queue.push(Factory.string)

      SystemTimer.timeout(1) do
        assert_nothing_raised{ subject.join }
      end
    end

    should "run its on-error callbacks when a shutdown error is raised while running" do
      exception = Factory.exception(DatWorkerPool::ShutdownError)
      error_method = setup_work_loop_to_raise_exception(exception)
      subject.start
      work_item = Factory.string
      @queue.push(work_item)

      assert_equal exception, subject.on_error_exception
      # the on-sleep callback will not include a work item if it errors
      exp = error_method == :on_sleep_error ? nil : work_item
      assert_equal exp, subject.on_error_work_item
    end

    should "run callbacks when its started" do
      assert_nil subject.first_on_start_call_order
      assert_nil subject.second_on_start_call_order
      assert_nil subject.first_on_sleep_call_order
      assert_nil subject.second_on_sleep_call_order

      subject.start

      assert_equal 1, subject.first_on_start_call_order
      assert_equal 2, subject.second_on_start_call_order
      assert_equal 3, subject.first_on_sleep_call_order
      assert_equal 4, subject.second_on_sleep_call_order
    end

    should "run its callbacks when work is pushed" do
      subject.start
      subject.reset_call_order

      assert_nil subject.first_on_sleep_call_order
      assert_nil subject.second_on_sleep_call_order
      assert_nil subject.first_before_work_call_order
      assert_nil subject.second_before_work_call_order
      assert_nil subject.work_call_order
      assert_nil subject.first_after_work_call_order
      assert_nil subject.second_after_work_call_order
      assert_nil subject.first_on_wakeup_call_order
      assert_nil subject.second_on_wakeup_call_order

      @queue.push(Factory.string)

      assert_equal 1, subject.first_on_wakeup_call_order
      assert_equal 2, subject.second_on_wakeup_call_order
      assert_equal 3, subject.first_before_work_call_order
      assert_equal 4, subject.second_before_work_call_order
      assert_equal 5, subject.work_call_order
      assert_equal 6, subject.first_after_work_call_order
      assert_equal 7, subject.second_after_work_call_order
      assert_equal 8, subject.first_on_sleep_call_order
      assert_equal 9, subject.second_on_sleep_call_order
    end

    should "run callbacks when its shutdown" do
      subject.start
      subject.reset_call_order

      assert_nil subject.first_on_wakeup_call_order
      assert_nil subject.second_on_wakeup_call_order
      assert_nil subject.first_on_shutdown_call_order
      assert_nil subject.second_on_shutdown_call_order

      shutdown_worker_queue_and_wait_for_thread_to_stop

      assert_equal 1, subject.first_on_wakeup_call_order
      assert_equal 2, subject.second_on_wakeup_call_order
      assert_equal 3, subject.first_on_shutdown_call_order
      assert_equal 4, subject.second_on_shutdown_call_order
    end

    ERROR_METHODS = [
      :on_sleep_error,
      :on_wakeup_error,
      :before_work_error,
      :work_error,
      :after_work_error
    ].freeze
    def setup_work_loop_to_raise_exception(exception)
      error_method = ERROR_METHODS.choice
      @worker.send("#{error_method}=", exception)
      error_method
    end

  end

  class TestWorker
    include DatWorkerPool::Worker

    attr_accessor :first_on_start_call_order, :second_on_start_call_order
    attr_accessor :first_on_shutdown_call_order, :second_on_shutdown_call_order
    attr_accessor :first_on_sleep_call_order, :second_on_sleep_call_order
    attr_accessor :first_on_wakeup_call_order, :second_on_wakeup_call_order
    attr_accessor :first_before_work_call_order, :second_before_work_call_order
    attr_accessor :first_after_work_call_order, :second_after_work_call_order
    attr_accessor :work_call_order

    attr_accessor :before_work_item_worked_on, :after_work_item_worked_on
    attr_accessor :item_worked_on

    attr_accessor :on_sleep_error, :on_wakeup_error
    attr_accessor :before_work_error, :after_work_error
    attr_accessor :work_error

    attr_accessor :on_error_exception, :on_error_work_item

    # TODO - change once these are instance evald
    on_start{ |w| w.first_on_start_call_order  = w.next_call_order }
    on_start{ |w| w.second_on_start_call_order = w.next_call_order }

    on_shutdown{ |w| w.first_on_shutdown_call_order  = w.next_call_order }
    on_shutdown{ |w| w.second_on_shutdown_call_order = w.next_call_order }

    on_sleep{ |w| w.first_on_sleep_call_order  = w.next_call_order }
    on_sleep do |w|
      w.raise_error_if_set(:on_sleep)
      w.second_on_sleep_call_order = w.next_call_order
    end

    on_wakeup{ |w| w.first_on_wakeup_call_order  = w.next_call_order }
    on_wakeup do |w|
      w.raise_error_if_set(:on_wakeup)
      w.second_on_wakeup_call_order = w.next_call_order
    end

    before_work{ |w, work_item| w.first_before_work_call_order = w.next_call_order }
    before_work do |w, work_item|
      w.raise_error_if_set(:before_work)
      w.before_work_item_worked_on    = work_item
      w.second_before_work_call_order = w.next_call_order
    end

    after_work{ |w, work_item| w.first_after_work_call_order = w.next_call_order }
    after_work do |w, work_item|
      w.raise_error_if_set(:after_work)
      w.after_work_item_worked_on    = work_item
      w.second_after_work_call_order = w.next_call_order
    end

    on_error do |w, exception, work_item|
      w.on_error_exception = exception
      w.on_error_work_item = work_item
    end

    def work!(work_item)
      raise_error_if_set(:work)
      @work_called     = true
      @item_worked_on  = work_item
      @work_call_order = next_call_order
    end

    def work_called; !!@work_called; end

    def reset_call_order
      @order = 0
      @first_on_start_call_order     = nil
      @second_on_start_call_order    = nil
      @first_on_shutdown_call_order  = nil
      @second_on_shutdown_call_order = nil
      @first_on_sleep_call_order     = nil
      @second_on_sleep_call_order    = nil
      @first_on_wakeup_call_order    = nil
      @second_on_wakeup_call_order   = nil
      @first_before_work_call_order  = nil
      @second_before_work_call_order = nil
      @first_after_work_call_order   = nil
      @second_after_work_call_order  = nil
      @work_call_order               = nil
    end

    def next_call_order; @order = (@order || 0) + 1; end

    # we want to unset the error method if its set to avoid the thread looping
    # very quickly because it doesn't sleep on `queue.pop`
    def raise_error_if_set(type)
      if (error = self.send("#{type}_error"))
        self.send("#{type}_error=", nil)
        Thread.current.raise error
      end
    end
  end

  class ThreadSpy
    attr_reader :join_seconds, :join_called
    attr_reader :raised_exception

    def initialize
      @join_seconds     = nil
      @join_called      = false
      @raised_exception = nil
    end

    def join(seconds = nil)
      @join_seconds = seconds
      @join_called  = true
    end

    def raise(exception)
      @raised_exception = exception
    end

    def alive?; true; end
  end

end
