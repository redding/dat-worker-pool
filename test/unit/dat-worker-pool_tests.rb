require 'assert'
require 'dat-worker-pool'

require 'system_timer'

class DatWorkerPool

  class UnitTests < Assert::Context
    desc "DatWorkerPool"
    setup do
      @worker_pool_class = DatWorkerPool
    end
    subject{ @worker_pool_class }

    should "know its default and min number of workers" do
      assert_equal 1, DEFAULT_NUM_WORKERS
      assert_equal 1, MIN_WORKERS
    end

  end

  class InitTests < UnitTests
    desc "when init"
    setup do
      @worker_pool = @worker_pool_class.new(:do_work_proc => proc{ })
    end
    subject{ @worker_pool }

    should have_readers :num_workers, :logger
    should have_readers :queue
    should have_readers :on_worker_error_callbacks
    should have_readers :on_worker_start_callbacks, :on_worker_shutdown_callbacks
    should have_readers :on_worker_sleep_callbacks, :on_worker_wakeup_callbacks
    should have_readers :before_work_callbacks, :after_work_callbacks
    should have_imeths :add_work, :start, :shutdown
    should have_imeths :work_items, :waiting
    should have_imeths :worker_available?
    should have_imeths :queue_empty?
    should have_imeths :on_queue_pop_callbacks, :on_queue_push_callbacks
    should have_imeths :on_queue_pop, :on_queue_push
    should have_imeths :on_worker_error
    should have_imeths :on_worker_start, :on_worker_shutdown
    should have_imeths :on_worker_sleep, :on_worker_wakeup
    should have_imeths :before_work, :after_work

    should "default its attributes" do
      assert_equal DEFAULT_NUM_WORKERS, subject.num_workers
      assert_instance_of NullLogger, subject.logger
      assert_instance_of DatWorkerPool::DefaultQueue, subject.queue

      assert_equal [], subject.on_worker_error_callbacks
      assert_equal [], subject.on_worker_start_callbacks
      assert_equal 1, subject.on_worker_shutdown_callbacks.size
      assert_instance_of Proc, subject.on_worker_shutdown_callbacks.first
      assert_equal 1, subject.on_worker_sleep_callbacks.size
      assert_instance_of Proc, subject.on_worker_sleep_callbacks.first
      assert_equal 1, subject.on_worker_wakeup_callbacks.size
      assert_instance_of Proc, subject.on_worker_wakeup_callbacks.first
      assert_equal [], subject.before_work_callbacks
      assert_equal [], subject.after_work_callbacks
    end

    should "allow customizing it by passing options" do
      num_workers = Factory.integer
      logger      = Logger.new(STDOUT)
      worker_pool = @worker_pool_class.new({
        :num_workers  => num_workers,
        :logger       => logger,
        :do_work_proc => proc{ }
      })
      assert_equal num_workers, worker_pool.num_workers
      assert_equal logger,      worker_pool.logger
    end

    should "demeter its queue callbacks" do
      callback = proc{ }
      subject.on_queue_pop(&callback)
      assert_equal [callback], subject.on_queue_pop_callbacks
      callback = proc{ }
      subject.on_queue_push(&callback)
      assert_equal [callback], subject.on_queue_push_callbacks
    end

    # TODO - stub queue once we can pass a queue instance to worker pool
    should "know if its queue is empty or not" do
      assert_equal true, subject.queue_empty?
      subject.add_work Factory.string
      assert_equal false, subject.queue_empty?
    end

    # TODO - once we pass queue, test shutdown changes on queue
    should "start its queue when its started" do
      assert_false subject.queue.shutdown?
      subject.start
      assert_false subject.queue.shutdown?
      subject.shutdown
      assert_true subject.queue.shutdown?
      subject.start
      assert_false subject.queue.shutdown?
    end

    # TODO - once we pass worker class
    should "spawn workers when its started" do
      skip
    end

    should "raise an argument error if given an invalid number of workers" do
      assert_raises(ArgumentError) do
        @worker_pool_class.new({
          :num_workers  => [0, (Factory.integer * -1)].choice,
          :do_work_proc => proc{ }
        })
      end
    end

  end

  # TODO - change once a custom worker class can be provided, these probably go
  # away
  class WorkerCallbackTests < UnitTests
    desc "worker callbacks"
    setup do
      @error_called       = false
      @start_called       = false
      @shutdown_called    = false
      @sleep_called       = false
      @wakeup_called      = false
      @before_work_called = false
      @after_work_called  = false

      @worker_pool = @worker_pool_class.new({
        :do_work_proc => proc do |work|
          raise work if work == 'error'
        end
      })
      @worker_pool.on_worker_error{ @error_called = true }
      @worker_pool.on_worker_start{ @start_called = true }
      @worker_pool.on_worker_shutdown{ @shutdown_called = true }
      @worker_pool.on_worker_sleep{ @sleep_called = true }
      @worker_pool.on_worker_wakeup{ @wakeup_called = true }
      @worker_pool.before_work{ @before_work_called = true }
      @worker_pool.after_work{ @after_work_called = true }
    end
    subject{ @worker_pool }

    should "call the worker callbacks as workers wait or wakeup" do
      assert_false @start_called
      assert_false @sleep_called
      subject.start
      assert_true @start_called
      assert_true @sleep_called

      @sleep_called = false
      assert_false @wakeup_called
      assert_false @before_work_called
      assert_false @after_work_called
      subject.add_work 'work'
      assert_true @wakeup_called
      assert_true @before_work_called
      assert_true @after_work_called
      assert_true @sleep_called

      @before_work_called = false
      @after_work_called  = false
      assert_false @before_work_called
      assert_false @error_called
      assert_false @after_work_called
      subject.add_work 'error'
      assert_true @before_work_called
      assert_true @error_called
      assert_false @after_work_called

      @wakeup_called = false
      assert_false @shutdown_called
      subject.shutdown
      assert_true @wakeup_called
      assert_true @shutdown_called
    end

  end

  # TODO - remove once we pass a custom worker class and queue `add_work` can
  # be tested in the `InitTests` once we can pass a queue; the `process` method
  # should go away once we can pass a worker class
  class AddWorkAndProcessItTests < UnitTests
    desc "add_work and process"
    setup do
      @result = nil
      @worker_pool = @worker_pool_class.new({
        :do_work_proc => proc{ |work| @result = (2 / work) }
      })
      @worker_pool.start
    end
    subject{ @worker_pool }

    should "add the work and processed it by calling the passed block" do
      subject.add_work 2
      sleep 0.1 # ensure worker thread get's a chance to run
      assert_equal 1, @result
    end

    # TODO - move to worker tests ? or remove if we already have it
    should "swallow exceptions, so workers don't end unexpectedly" do
      subject.add_work 0
      worker = subject.instance_variable_get("@workers").first
      sleep 0.1

      assert_equal 1, subject.waiting
      assert worker.instance_variable_get("@thread").alive?
    end

  end

  # TODO - once we can pass a custom worker class
  class ShutdownTests < UnitTests

  end

  class ForceShutdownSetupTests < UnitTests
    desc "forced shutdown"
    setup do
      # this makes the worker pool force shutdown, by raising timeout error we
      # are saying the graceful shutdown timed out
      Assert.stub(OptionalTimeout, :new){ raise TimeoutError }

      @num_workers = Factory.integer(4)
      @worker_pool = DatWorkerPool.new({
        :num_workers  => @num_workers,
        :do_work_proc => proc{ }
      })
    end
    subject{ @worker_pool }

  end

  class ForcedShutdownTests < ForceShutdownSetupTests
    setup do
      @workers = @num_workers.times.map do
        ForceShutdownSpyWorker.new(@worker_pool.queue)
      end
      stub_workers = @workers.dup
      Assert.stub(Worker, :new){ stub_workers.pop }

      @worker_pool.start
    end

    should "force workers to shutdown by raising an error in their thread" do
      subject.shutdown(Factory.integer)

      @workers.each do |worker|
        assert_instance_of ShutdownError, worker.raised_error
        assert_true worker.joined
      end
    end

  end

  class ForcedShutdownWithErrorsWhileJoiningTests < ForceShutdownSetupTests
    desc "forced shutdown with errors while joining worker threads"
    setup do
      @workers = @num_workers.times.map do
        ForceShutdownJoinErrorWorker.new(@worker_pool.queue)
      end
      Assert.stub(Worker, :new){ @workers.pop }

      @worker_pool.start
    end

    should "not raise any errors and continue shutting down" do
      # if this is broken its possible it can create an infinite loop, so we
      # time it out and let it throw an exception
      SystemTimer.timeout(1) do
        assert_nothing_raised{ subject.shutdown(Factory.integer) }
      end
    end

  end

  class NullLoggerTests < UnitTests
    desc "NullLogger"
    setup do
      @logger = NullLogger.new
    end
    subject{ @logger }

    should have_imeths :debug, :info, :error

  end

  class ForceShutdownSpyWorker < Worker
    attr_reader :raised_error, :joined

    def start; end
    def raise(error); @raised_error = error; end
    def join; @joined = true; end
  end

  # this creates a rare scenario where as we are joining a worker, it throws
  # an error; this can happen when force shutting down a worker; we raise an
  # error in the worker thread to force it to shut down, the error can be raised
  # at any point in the worker thread, including its ensure block in its
  # `work_loop`, if this happens, we will get the error raised when we `join`
  # the worker thread
  class ForceShutdownJoinErrorWorker < ForceShutdownSpyWorker
    def join; raise Factory.exception; end
  end

end
