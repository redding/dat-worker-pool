require 'assert'
require 'dat-worker-pool/worker'

require 'much-timeout'
require 'dat-worker-pool/default_queue'
require 'dat-worker-pool/runner'
require 'test/support/thread_spies'

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
    should have_imeths :on_available_callbacks, :on_unavailable_callbacks
    should have_imeths :on_error_callbacks
    should have_imeths :before_work_callbacks, :after_work_callbacks
    should have_imeths :on_start, :on_shutdown
    should have_imeths :on_available, :on_unavailable
    should have_imeths :on_error
    should have_imeths :before_work, :after_work
    should have_imeths :prepend_on_start, :prepend_on_shutdown
    should have_imeths :prepend_on_available, :prepend_on_unavailable
    should have_imeths :prepend_on_error
    should have_imeths :prepend_before_work, :prepend_after_work

    should "not have any callbacks by default" do
      assert_equal [], subject.on_start_callbacks
      assert_equal [], subject.on_shutdown_callbacks
      assert_equal [], subject.on_available_callbacks
      assert_equal [], subject.on_unavailable_callbacks
      assert_equal [], subject.on_error_callbacks
      assert_equal [], subject.before_work_callbacks
      assert_equal [], subject.after_work_callbacks
    end

    should "allow appending callbacks" do
      callback = proc{ Factory.string }
      # add a callback to each type to show we are appending
      subject.on_start_callbacks       << proc{ Factory.string }
      subject.on_shutdown_callbacks    << proc{ Factory.string }
      subject.on_available_callbacks   << proc{ Factory.string }
      subject.on_unavailable_callbacks << proc{ Factory.string }
      subject.on_error_callbacks       << proc{ Factory.string }
      subject.before_work_callbacks    << proc{ Factory.string }
      subject.after_work_callbacks     << proc{ Factory.string }

      subject.on_start(&callback)
      assert_equal callback, subject.on_start_callbacks.last

      subject.on_shutdown(&callback)
      assert_equal callback, subject.on_shutdown_callbacks.last

      subject.on_available(&callback)
      assert_equal callback, subject.on_available_callbacks.last

      subject.on_unavailable(&callback)
      assert_equal callback, subject.on_unavailable_callbacks.last

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
      subject.on_start_callbacks       << proc{ Factory.string }
      subject.on_shutdown_callbacks    << proc{ Factory.string }
      subject.on_available_callbacks   << proc{ Factory.string }
      subject.on_unavailable_callbacks << proc{ Factory.string }
      subject.on_error_callbacks       << proc{ Factory.string }
      subject.before_work_callbacks    << proc{ Factory.string }
      subject.after_work_callbacks     << proc{ Factory.string }

      subject.prepend_on_start(&callback)
      assert_equal callback, subject.on_start_callbacks.first

      subject.prepend_on_shutdown(&callback)
      assert_equal callback, subject.on_shutdown_callbacks.first

      subject.prepend_on_available(&callback)
      assert_equal callback, subject.on_available_callbacks.first

      subject.prepend_on_unavailable(&callback)
      assert_equal callback, subject.on_unavailable_callbacks.first

      subject.prepend_on_error(&callback)
      assert_equal callback, subject.on_error_callbacks.first

      subject.prepend_before_work(&callback)
      assert_equal callback, subject.before_work_callbacks.first

      subject.prepend_after_work(&callback)
      assert_equal callback, subject.after_work_callbacks.first
    end

  end

  class InitTests < UnitTests
    desc "when init"
    setup do
      @mutex    = Mutex.new
      @cond_var = ConditionVariable.new

      @queue  = DatWorkerPool::DefaultQueue.new.tap(&:dwp_start)
      @runner = DatWorkerPool::Runner.new({
        :logger        => TEST_LOGGER,
        :queue         => @queue,
        :worker_params => {
          :mutex    => @mutex,
          :cond_var => @cond_var
        }
      })
      @number = Factory.integer(10)

      # we only want to stub the first `Thread.new` call, not all calls;
      # stubbing all threads can generate unexpected behavior and specifically
      # causes `MuchTimeout` to not work correctly
      @worker_thread_built = false
      @thread_spy          = ThreadSpy.new
      Assert.stub(Thread, :new) do |&block|
        if !@worker_thread_built
          @worker_thread_built = true
          @thread_spy.tap{ |s| s.block = block }
        else
          Assert.stub_send(Thread, :new, &block)
        end
      end

      @available_worker = nil
      Assert.stub(@runner, :make_worker_available){ |w| @available_worker = w }
      @unavailable_worker = nil
      Assert.stub(@runner, :make_worker_unavailable){ |w| @unavailable_worker = w }

      @worker = TestWorker.new(@runner, @queue, @number)
    end
    teardown do
      shutdown_worker_queue_and_wait_for_thread_to_stop
    end
    subject{ @worker }

    should have_readers :dwp_number
    should have_imeths :dwp_start, :dwp_signal_shutdown
    should have_imeths :dwp_running?, :dwp_shutdown?
    should have_imeths :dwp_thread_alive?, :dwp_join, :dwp_raise

    should "know its queue and params" do
      assert_equal @number,               subject.instance_eval{ number }
      assert_equal @runner.worker_params, subject.instance_eval{ params }
      assert_equal @queue,                subject.instance_eval{ queue }
    end

    should "start a thread when its started" do
      thread = subject.dwp_start
      wait_for_worker_to_be_available

      assert_same @thread_spy, thread
      assert_true thread.alive?
    end

    should "make itself available when started" do
      subject.dwp_start
      wait_for_worker_to_be_available

      assert_same subject, @available_worker
      assert_not_nil subject.first_on_available_call_order
      assert_not_nil subject.second_on_available_call_order
    end

    should "know if its running and if its thread is alive or not" do
      assert_false subject.dwp_running?
      assert_false subject.dwp_thread_alive?

      subject.dwp_start
      wait_for_worker_to_be_available

      assert_true subject.dwp_running?
      assert_true subject.dwp_thread_alive?

      subject.dwp_signal_shutdown
      assert_false subject.dwp_running?
      assert_true  subject.dwp_thread_alive?

      shutdown_worker_queue_and_wait_for_thread_to_stop
      assert_false subject.dwp_running?
      assert_false subject.dwp_thread_alive?
    end

    should "make itself unavailable when its thread stops" do
      subject.dwp_start
      wait_for_worker_to_be_available

      shutdown_worker_queue_and_wait_for_thread_to_stop
      assert_same subject, @unavailable_worker
      assert_not_nil subject.first_on_unavailable_call_order
      assert_not_nil subject.second_on_unavailable_call_order
    end

    should "allow joining and raising on its thread" do
      subject.dwp_start
      wait_for_worker_to_be_available

      subject.dwp_join(0.1)
      assert_equal 0.1, @thread_spy.join_seconds
      assert_true @thread_spy.join_called

      exception = Factory.exception
      subject.dwp_raise(exception)
      assert_equal exception, @thread_spy.raised_exception
    end

    should "not allow joining or raising if it hasn't been started" do
      assert_nothing_raised{ subject.dwp_join(Factory.integer) }
      assert_false @thread_spy.join_called
      assert_nothing_raised{ subject.dwp_raise(Factory.exception) }
      assert_nil @thread_spy.raised_exception
    end

    should "make itself available and unavailable while running" do
      subject.dwp_start
      wait_for_worker_to_be_available

      @queue.dwp_push(Factory.string)
      wait_for_worker_to_work_and_then_be_available

      assert_same subject, @unavailable_worker
      assert_same subject, @available_worker
    end

    should "call its on-available and on-unavailable callbacks while running" do
      subject.dwp_start
      wait_for_worker_to_be_available

      @queue.dwp_push(Factory.string)
      wait_for_worker_to_work_and_then_be_available

      assert_not_nil subject.first_on_unavailable_call_order
      assert_not_nil subject.second_on_unavailable_call_order
      assert_not_nil subject.first_on_available_call_order
      assert_not_nil subject.second_on_available_call_order
    end

    should "make itself available/unavailable and run callbacks if it errors" do
      # these are the only errors that could interfere with it
      error_method = [:on_unavailable_error, :work_error].sample
      exception = Factory.exception
      subject.send("#{error_method}=", exception)

      subject.dwp_start
      wait_for_worker_to_be_available

      @queue.dwp_push(Factory.string)
      wait_for_worker_to_error_and_then_be_available

      assert_equal exception, subject.on_error_exception
      assert_same subject, @unavailable_worker
      assert_not_nil subject.first_on_unavailable_call_order
      assert_same subject, @available_worker
      assert_not_nil subject.first_on_available_call_order
    end

    should "call its `work` method on any pushed items while running" do
      assert_nil subject.item_worked_on
      subject.dwp_start
      wait_for_worker_to_be_available
      assert_nil subject.item_worked_on

      work_item = Factory.string
      @queue.dwp_push(work_item)
      wait_for_worker_to_work_and_then_be_available

      assert_same work_item, subject.before_work_item_worked_on
      assert_same work_item, subject.item_worked_on
      assert_same work_item, subject.after_work_item_worked_on
    end

    should "not call its `work` method if it pops a `nil` work item" do
      subject.dwp_start
      wait_for_worker_to_be_available

      @queue.dwp_push(nil)
      # we don't have any event to listen for because it should ignore `nil`
      # work items
      @thread_spy.join(JOIN_SECONDS)
      assert_false subject.work_called

      # when the queue is shutdown it returns `nil`, so we shouldn't call `work`
      # when shutting down
      shutdown_worker_queue_and_wait_for_thread_to_stop
      assert_false subject.work_called
    end

    should "run its on-error callbacks if it errors while starting" do
      exception = Factory.exception
      error_method = [:on_start_error, :on_available_error].sample
      subject.send("#{error_method}=", exception)

      subject.dwp_start
      wait_for_worker_thread_to_stop_and_rescue_if_expected_error(exception)

      assert_equal exception, subject.on_error_exception
      assert_nil subject.on_error_work_item
    end

    should "stop its thread if it errors while starting" do
      exception = Factory.exception
      error_method = [:on_start_error, :on_available_error].sample
      subject.send("#{error_method}=", exception)

      subject.dwp_start
      wait_for_worker_thread_to_stop_and_rescue_if_expected_error(exception)

      assert_false subject.dwp_thread_alive?
      assert_false subject.dwp_running?
    end

    should "run its on-error callbacks if it errors while shutting down" do
      exception = Factory.exception
      error_method = [:on_shutdown_error, :on_unavailable_error].sample
      subject.send("#{error_method}=", exception)

      subject.dwp_start
      shutdown_worker_and_queue
      wait_for_worker_thread_to_stop_and_rescue_if_expected_error(exception)

      assert_equal exception, subject.on_error_exception
      assert_nil subject.on_error_work_item
    end

    should "not stop its thread when an error occurs while running" do
      subject.dwp_start
      wait_for_worker_to_be_available

      exception = Factory.exception
      setup_work_loop_to_raise_exception(exception)

      @queue.dwp_push(Factory.string)
      wait_for_worker_to_error_and_then_be_available

      assert_true subject.dwp_thread_alive?
    end

    should "run its on-error callbacks if an error occurs while running" do
      exception = Factory.exception
      subject.dwp_start
      wait_for_worker_to_be_available

      setup_work_loop_to_raise_exception(exception)
      work_item = Factory.string
      @queue.dwp_push(work_item)
      wait_for_worker_to_error_and_then_be_available

      assert_equal exception, subject.on_error_exception
      assert_equal work_item, subject.on_error_work_item
    end

    should "not stop its thread if an error occurs in an on-error callback" do
      exception = Factory.exception
      subject.dwp_start
      wait_for_worker_to_be_available

      setup_work_loop_to_raise_exception(Factory.exception)
      subject.on_error_error = exception
      work_item = Factory.string
      @queue.dwp_push(work_item)
      wait_for_worker_to_error_and_then_be_available

      assert_true subject.dwp_thread_alive?
    end

    should "stop its thread when a shutdown error is raised while running" do
      exception = Factory.exception(DatWorkerPool::ShutdownError)
      subject.dwp_start
      wait_for_worker_to_be_available

      setup_work_loop_to_raise_exception(exception)
      @queue.dwp_push(Factory.string)

      wait_for_worker_thread_to_stop_and_rescue_if_expected_error(exception)
      assert_false subject.dwp_thread_alive?
    end

    should "stop its thread when a shutdown error is raised in an on-error callback" do
      exception = Factory.exception(DatWorkerPool::ShutdownError)
      subject.dwp_start
      wait_for_worker_to_be_available

      setup_work_loop_to_raise_exception(Factory.exception)
      subject.on_error_error = exception
      @queue.dwp_push(Factory.string)

      wait_for_worker_thread_to_stop_and_rescue_if_expected_error(exception)
      assert_false subject.dwp_thread_alive?
    end

    should "only run its on-error callbacks when shutdown error is raised with a work item" do
      exception = Factory.exception(DatWorkerPool::ShutdownError)
      subject.dwp_start
      wait_for_worker_to_be_available

      error_method = ERROR_METHODS.reject{ |e| e == :on_available_error }.sample
      subject.send("#{error_method}=", exception)
      work_item = Factory.string
      @queue.dwp_push(work_item)
      wait_for_worker_thread_to_stop_and_rescue_if_expected_error(exception)

      assert_equal exception, subject.on_error_exception
      assert_equal work_item, subject.on_error_work_item
    end

    should "run callbacks when its started" do
      assert_nil subject.first_on_start_call_order
      assert_nil subject.second_on_start_call_order
      assert_nil subject.first_on_available_call_order
      assert_nil subject.second_on_available_call_order

      subject.dwp_start
      wait_for_worker_to_be_available

      assert_equal 1, subject.first_on_start_call_order
      assert_equal 2, subject.second_on_start_call_order
      assert_equal 3, subject.first_on_available_call_order
      assert_equal 4, subject.second_on_available_call_order
    end

    should "run its callbacks when work is pushed" do
      subject.dwp_start
      wait_for_worker_to_be_available
      subject.reset_call_order

      assert_nil subject.first_on_unavailable_call_order
      assert_nil subject.second_on_unavailable_call_order
      assert_nil subject.first_before_work_call_order
      assert_nil subject.second_before_work_call_order
      assert_nil subject.work_call_order
      assert_nil subject.first_after_work_call_order
      assert_nil subject.second_after_work_call_order
      assert_nil subject.first_on_available_call_order
      assert_nil subject.second_on_available_call_order

      @queue.dwp_push(Factory.string)
      wait_for_worker_to_work_and_then_be_available

      assert_equal 1, subject.first_on_unavailable_call_order
      assert_equal 2, subject.second_on_unavailable_call_order
      assert_equal 3, subject.first_before_work_call_order
      assert_equal 4, subject.second_before_work_call_order
      assert_equal 5, subject.work_call_order
      assert_equal 6, subject.first_after_work_call_order
      assert_equal 7, subject.second_after_work_call_order
      assert_equal 8, subject.first_on_available_call_order
      assert_equal 9, subject.second_on_available_call_order
    end

    should "run callbacks when its shutdown" do
      subject.dwp_start
      wait_for_worker_to_be_available
      subject.reset_call_order

      assert_nil subject.first_on_unavailable_call_order
      assert_nil subject.second_on_unavailable_call_order
      assert_nil subject.first_on_shutdown_call_order
      assert_nil subject.second_on_shutdown_call_order

      shutdown_worker_queue_and_wait_for_thread_to_stop

      assert_equal 1, subject.first_on_unavailable_call_order
      assert_equal 2, subject.second_on_unavailable_call_order
      assert_equal 3, subject.first_on_shutdown_call_order
      assert_equal 4, subject.second_on_shutdown_call_order
    end

    should "run its callbacks when an error occurs while making itself unavailable" do
      subject.dwp_start
      wait_for_worker_to_be_available
      subject.reset_call_order

      subject.on_unavailable_error = Factory.exception
      @queue.dwp_push(Factory.string)
      wait_for_worker_to_error_and_then_be_available

      assert_equal 1, subject.first_on_unavailable_call_order
      assert_equal 2, subject.on_error_call_order
      assert_equal 3, subject.first_on_available_call_order
      assert_equal 4, subject.second_on_available_call_order
      assert_nil subject.second_on_unavailable_call_order
      assert_nil subject.first_before_work_call_order
      assert_nil subject.second_before_work_call_order
      assert_nil subject.work_call_order
      assert_nil subject.first_after_work_call_order
      assert_nil subject.second_after_work_call_order
    end

    should "run its callbacks when an error occurs while working" do
      subject.dwp_start
      wait_for_worker_to_be_available
      subject.reset_call_order

      subject.work_error = Factory.exception
      @queue.dwp_push(Factory.string)
      wait_for_worker_to_error_and_then_be_available

      assert_equal 1, subject.first_on_unavailable_call_order
      assert_equal 2, subject.second_on_unavailable_call_order
      assert_equal 3, subject.first_before_work_call_order
      assert_equal 4, subject.second_before_work_call_order
      assert_equal 5, subject.on_error_call_order
      assert_equal 6, subject.first_on_available_call_order
      assert_equal 7, subject.second_on_available_call_order
      assert_nil subject.work_call_order
      assert_nil subject.first_after_work_call_order
      assert_nil subject.second_after_work_call_order
    end

    should "run its callbacks when an error occurs while making itself available" do
      subject.dwp_start
      wait_for_worker_to_be_available
      subject.reset_call_order

      subject.on_available_error = Factory.exception
      @queue.dwp_push(Factory.string)
      wait_for_worker_to_error_and_then_be_available

      assert_equal 1, subject.first_on_unavailable_call_order
      assert_equal 2, subject.second_on_unavailable_call_order
      assert_equal 3, subject.first_before_work_call_order
      assert_equal 4, subject.second_before_work_call_order
      assert_equal 5, subject.work_call_order
      assert_equal 6, subject.first_after_work_call_order
      assert_equal 7, subject.second_after_work_call_order
      assert_equal 8, subject.first_on_available_call_order
      assert_equal 9, subject.on_error_call_order
      assert_nil subject.second_on_available_call_order
    end

    ERROR_METHODS = [
      :on_available_error,
      :on_unavailable_error,
      :before_work_error,
      :work_error,
      :after_work_error
    ].freeze
    def setup_work_loop_to_raise_exception(exception)
      error_method = ERROR_METHODS.sample
      @worker.send("#{error_method}=", exception)
      error_method
    end

    # this could loop forever so ensure it doesn't by using a timeout
    def wait_for_worker(&block)
      MuchTimeout.timeout(1) do
        @mutex.synchronize{ @cond_var.wait(@mutex) } while !block.call
      end
    end

    def wait_for_worker_to_be_available
      wait_for_worker{ @worker.first_on_available_call_order }
    end

    def wait_for_worker_to_work_and_then_be_available
      wait_for_worker do
        @worker.work_called && @worker.first_on_available_call_order
      end
    end

    def wait_for_worker_to_error_and_then_be_available
      wait_for_worker do
        @worker.on_error_exception && @worker.first_on_available_call_order
      end
    end

    def shutdown_worker_queue_and_wait_for_thread_to_stop
      shutdown_worker_and_queue
      wait_for_worker_thread_to_stop
    end

    def shutdown_worker_and_queue
      @worker.dwp_signal_shutdown
      @queue.dwp_shutdown
    end

    def wait_for_worker_thread_to_stop
      return unless @worker.dwp_thread_alive?
      MuchTimeout.timeout(1){ @worker.dwp_join }
    end

    # this is needed because errors will be re-raised when the thread is joined
    # and in some cases we expect these errors because we are manually raising
    # them, this checks if they are the expected exception and won't re-raise
    # them; to check if they are the expected exception, we have to use the
    # class and message because when the thread raises an error on join it is a
    # different instance with a different backtrace (so we can't use `==`)
    def wait_for_worker_thread_to_stop_and_rescue_if_expected_error(exception)
      begin
        wait_for_worker_thread_to_stop
      rescue exception.class => caught_exception
        unless caught_exception.class   == exception.class &&
               caught_exception.message == exception.message
          raise(caught_exception)
        end
      end
    end

  end

  class TestHelperTests < UnitTests
    desc "TestHelpers"
    setup do
      @mutex    = Mutex.new
      @cond_var = ConditionVariable.new

      @worker_class = TestWorker
      @options = {
        :logger => TEST_LOGGER || Logger.new("/dev/null"),
        :queue  => DatWorkerPool::DefaultQueue.new,
        :params => {
          :mutex    => @mutex,
          :cond_var => @cond_var
        }
      }

      @context_class = Class.new do
        include DatWorkerPool::Worker::TestHelpers
      end
      @context = @context_class.new
    end
    subject{ @context }

    should have_imeths :test_runner

    should "build a test runner using `test_runner`" do
      test_runner = subject.test_runner(@worker_class, @options)

      assert_instance_of TestHelpers::TestRunner, test_runner
      assert_equal @worker_class,     test_runner.worker_class
      assert_equal @options[:queue],  test_runner.queue
      assert_equal @options[:params], test_runner.dwp_runner.worker_params
    end

  end

  class TestRunnerTests < TestHelperTests
    desc "TestRunner"
    setup do
      @test_runner = TestHelpers::TestRunner.new(@worker_class, @options)

      dwp_runner = @test_runner.dwp_runner
      @unavailable_worker = nil
      Assert.stub(dwp_runner, :make_worker_unavailable){ |w| @unavailable_worker = w }
      @available_worker = nil
      Assert.stub(dwp_runner, :make_worker_available){ |w| @available_worker = w }
    end
    subject{ @test_runner }

    should have_readers :worker_class, :worker
    should have_readers :queue, :dwp_runner
    should have_imeths :run, :work, :error
    should have_imeths :start, :shutdown
    should have_imeths :make_unavailable, :make_available

    should "know its attributes" do
      assert_equal @worker_class,    subject.worker_class
      assert_equal @options[:queue], subject.queue
    end

    should "build a dat-worker-pool runner" do
      dwp_runner = subject.dwp_runner
      assert_instance_of DatWorkerPool::Runner, dwp_runner
      assert_equal DatWorkerPool::MIN_WORKERS,  dwp_runner.num_workers
      assert_equal subject.queue,               dwp_runner.queue
      assert_equal @options[:logger],           dwp_runner.logger_proxy.logger
      assert_equal subject.worker_class,        dwp_runner.worker_class
      assert_equal @options[:params],           dwp_runner.worker_params
    end

    should "build a worker" do
      assert_instance_of @worker_class, subject.worker
      assert_equal 1, subject.worker.dwp_number
    end

    should "run a workers life-cycle using `run`" do
      work_item = Factory.string
      subject.run(work_item)

      worker = subject.worker
      assert_not_nil worker.first_on_start_call_order
      assert_same worker, @unavailable_worker
      assert_not_nil worker.first_on_unavailable_call_order
      assert_equal work_item, worker.before_work_item_worked_on
      assert_equal work_item, worker.item_worked_on
      assert_equal work_item, worker.after_work_item_worked_on
      assert_same worker, @available_worker
      assert_not_nil worker.first_on_available_call_order
      assert_not_nil worker.first_on_shutdown_call_order
    end

    should "call its workers work method using `work`" do
      work_item = Factory.string
      subject.work(work_item)

      worker = subject.worker
      assert_equal work_item, worker.before_work_item_worked_on
      assert_equal work_item, worker.item_worked_on
      assert_equal work_item, worker.after_work_item_worked_on
    end

    should "call its workers on-error callbacks using `error`" do
      exception = Factory.exception
      subject.error(exception)

      worker = subject.worker
      assert_equal exception, worker.on_error_exception
      assert_nil worker.on_error_work_item

      work_item = Factory.string
      subject.error(exception, work_item)
      assert_equal work_item, worker.on_error_work_item
    end

    should "call its workers on-start callbacks using `start`" do
      subject.start
      assert_not_nil subject.worker.first_on_start_call_order
    end

    should "call its workers on-shutdown callbacks using `shutdown`" do
      subject.shutdown
      assert_not_nil subject.worker.first_on_shutdown_call_order
    end

    should "call its workers make unavailable method using `make_unavailable`" do
      subject.make_unavailable
      assert_same subject.worker, @unavailable_worker
      assert_not_nil subject.worker.first_on_unavailable_call_order
    end

    should "call its workers make available method using `make_available`" do
      subject.make_available
      assert_same subject.worker, @available_worker
      assert_not_nil subject.worker.first_on_available_call_order
    end

  end

  class TestWorker
    include DatWorkerPool::Worker

    attr_reader :first_on_start_call_order, :second_on_start_call_order
    attr_reader :first_on_shutdown_call_order, :second_on_shutdown_call_order
    attr_reader :first_on_available_call_order, :second_on_available_call_order
    attr_reader :first_on_unavailable_call_order, :second_on_unavailable_call_order
    attr_reader :first_before_work_call_order, :second_before_work_call_order
    attr_reader :first_after_work_call_order, :second_after_work_call_order
    attr_reader :work_call_order, :on_error_call_order

    attr_reader :before_work_item_worked_on, :after_work_item_worked_on
    attr_reader :item_worked_on

    attr_accessor :on_start_error, :on_shutdown_error
    attr_accessor :on_available_error, :on_unavailable_error
    attr_accessor :before_work_error, :after_work_error
    attr_accessor :work_error, :on_error_error

    attr_reader :on_error_exception, :on_error_work_item

    on_start{ @first_on_start_call_order  = next_call_order }
    on_start do
      raise_error_if_set(:on_start)
      @second_on_start_call_order = next_call_order
    end

    on_shutdown{ @first_on_shutdown_call_order  = next_call_order }
    on_shutdown do
      raise_error_if_set(:on_shutdown)
      @second_on_shutdown_call_order = next_call_order
    end

    on_available{ @first_on_available_call_order = next_call_order }
    on_available{ signal_test_suite_thread }
    on_available do
      raise_error_if_set(:on_available)
      @second_on_available_call_order = next_call_order
    end

    on_unavailable{ @first_on_unavailable_call_order = next_call_order }
    on_unavailable do
      raise_error_if_set(:on_unavailable)
      @second_on_unavailable_call_order = next_call_order
    end

    before_work{ |work_item| @first_before_work_call_order = next_call_order }
    before_work do |work_item|
      raise_error_if_set(:before_work)
      @before_work_item_worked_on    = work_item
      @second_before_work_call_order = next_call_order
    end

    after_work{ |work_item| @first_after_work_call_order = next_call_order }
    after_work do |work_item|
      raise_error_if_set(:after_work)
      @after_work_item_worked_on    = work_item
      @second_after_work_call_order = next_call_order
    end

    on_error do |exception, work_item|
      @on_error_exception  = exception
      @on_error_work_item  = work_item
    end
    on_error{ signal_test_suite_thread }
    on_error do
      raise_error_if_set(:on_error)
      @on_error_call_order = next_call_order
    end


    def work_called; !!@work_called; end

    def reset_call_order
      @order = 0
      @first_on_start_call_order        = nil
      @second_on_start_call_order       = nil
      @first_on_shutdown_call_order     = nil
      @second_on_shutdown_call_order    = nil
      @first_on_available_call_order    = nil
      @second_on_available_call_order   = nil
      @first_on_unavailable_call_order  = nil
      @second_on_unavailable_call_order = nil
      @first_before_work_call_order     = nil
      @second_before_work_call_order    = nil
      @first_after_work_call_order      = nil
      @second_after_work_call_order     = nil
      @work_call_order                  = nil
    end

    private

    def work!(work_item)
      raise_error_if_set(:work)
      @work_called     = true
      @item_worked_on  = work_item
      @work_call_order = next_call_order
    end

    def next_call_order; @order = (@order || 0) + 1; end

    # we want to unset the error method if its set to avoid the thread looping
    # very quickly because it doesn't available on `queue.pop`
    def raise_error_if_set(type)
      if (error = self.send("#{type}_error"))
        self.send("#{type}_error=", nil)
        Thread.current.raise error
      end
    end

    def signal_test_suite_thread
      params[:mutex].synchronize{ params[:cond_var].signal }
    end
  end

end
