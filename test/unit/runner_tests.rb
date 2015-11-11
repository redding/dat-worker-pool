require 'assert'
require 'dat-worker-pool/runner'

require 'dat-worker-pool/default_queue'
require 'test/support/thread_spies'

class DatWorkerPool::Runner

  class UnitTests < Assert::Context
    desc "DatWorkerPool::Runner"
    setup do
      @runner_class = DatWorkerPool::Runner
    end
    subject{ @runner_class }

  end

  class InitTests < UnitTests
    desc "when init"
    setup do
      @num_workers   = Factory.integer(4)
      @queue         = DatWorkerPool::DefaultQueue.new
      @worker_class  = WorkerSpy
      @worker_params = { Factory.string => Factory.string }

      @mutex_spy = MutexSpy.new
      Assert.stub(Mutex, :new){ @mutex_spy }

      @options = {
        :num_workers   => @num_workers,
        :queue         => @queue,
        :worker_class  => @worker_class,
        :worker_params => @worker_params
      }
      @runner = @runner_class.new(@options)
    end
    subject{ @runner }

    should have_readers :num_workers, :worker_class, :worker_params
    should have_readers :queue, :workers
    should have_imeths :start, :shutdown
    should have_imeths :workers_waiting_count
    should have_imeths :increment_worker_waiting, :decrement_worker_waiting
    should have_imeths :despawn_worker

    should "know its attributes" do
      assert_equal @num_workers,   subject.num_workers
      assert_equal @worker_class,  subject.worker_class
      assert_equal @worker_params, subject.worker_params
      assert_equal @queue,         subject.queue
    end

    should "default its workers to an empty array" do
      assert_equal [], subject.workers
    end

    should "demeter its workers waiting" do
      assert_equal 0, subject.workers_waiting_count
      subject.increment_worker_waiting
      assert_equal 1, subject.workers_waiting_count
      subject.decrement_worker_waiting
      assert_equal 0, subject.workers_waiting_count
    end

    should "start its queue when its started" do
      assert_false @queue.running?
      subject.start
      assert_true @queue.running?
    end

    should "spawn workers when its started" do
      subject.start
      assert_equal @num_workers, subject.workers.size
      subject.workers.each do |worker|
        assert_instance_of @worker_class, worker
        assert_equal subject, worker.runner
        assert_equal @queue,  worker.queue
        assert_true worker.running?
      end
    end

    should "lock access to its workers when started" do
      assert_false @mutex_spy.synchronize_called
      subject.start
      assert_true @mutex_spy.synchronize_called
    end

  end

  class StartedTests < InitTests
    desc "and started"
    setup do
      @runner.start
      @mutex_spy.synchronize_called = false # reset so we can test it below
    end

    should "remove a worker from its workers using `despawn_worker`" do
      worker = subject.workers.choice

      assert_false @mutex_spy.synchronize_called
      subject.despawn_worker(worker)
      assert_true @mutex_spy.synchronize_called
      assert_not_includes worker, subject.workers
    end

  end

  class ShutdownSetupTests < InitTests
    desc "and started and shutdown"

  end

  class ShutdownTests < ShutdownSetupTests
    setup do
      @timeout_seconds         = nil
      @optional_timeout_called = false
      # this acts as a spy but also keeps the shutdown from ever timing out
      Assert.stub(OptionalTimeout, :new) do |secs, &block|
        @timeout_seconds         = secs
        @optional_timeout_called = true
        block.call
      end

      @runner.start
    end

    should "optionally timeout when shutdown" do
      subject.shutdown
      assert_nil @timeout_seconds
      assert_true @optional_timeout_called

      @optional_timeout_called = false
      seconds = Factory.integer
      subject.shutdown(seconds)
      assert_equal seconds, @timeout_seconds
      assert_true @optional_timeout_called
    end

    should "shutdown all of its workers" do
      subject.workers.each do |worker|
        assert_false worker.shutdown?
      end
      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      subject.workers.each do |worker|
        assert_true worker.shutdown?
      end
    end

    should "shutdown its queue" do
      assert_false @queue.shutdown?
      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      assert_true @queue.shutdown?
    end

    should "join its workers waiting for them to finish" do
      subject.workers.each do |worker|
        assert_false worker.join_called
      end
      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      subject.workers.each do |worker|
        assert_true worker.join_called
      end
    end

  end

  class ForceShutdownSetupTests < ShutdownSetupTests
    desc "which times out and is forced to shutdown"
    setup do
      # this makes the runner force shutdown, by raising timeout error we
      # are saying the graceful shutdown timed out
      Assert.stub(OptionalTimeout, :new){ raise DatWorkerPool::TimeoutError }
    end

  end

  class ForcedShutdownTests < ForceShutdownSetupTests
    setup do
      @options[:worker_class] = ForceShutdownSpyWorker
      @runner = @runner_class.new(@options)
      @runner.start

      @workers = @runner.workers.dup
    end

    should "force workers to shutdown by raising an error in their thread" do
      subject.shutdown(Factory.integer)

      @workers.each do |worker|
        assert_instance_of DatWorkerPool::ShutdownError, worker.raised_error
        assert_true worker.join_called
      end
    end

  end

  class ForcedShutdownWithErrorsWhileJoiningTests < ForceShutdownSetupTests
    desc "forced shutdown with errors while joining worker threads"
    setup do
      @options[:worker_class] = ForceShutdownJoinErrorWorker
      @runner = @runner_class.new(@options)
      @runner.start
    end

    should "not raise any errors and continue shutting down" do
      # if this is broken its possible it can create an infinite loop, so we
      # time it out and let it throw an exception
      SystemTimer.timeout(1) do
        assert_nothing_raised{ subject.shutdown(Factory.integer) }
      end
    end

  end

  class WorkerSpy
    include DatWorkerPool::Worker

    attr_reader :runner, :queue
    attr_reader :join_called

    def initialize(*args)
      super
      @join_called = false
    end

    def join(*args); @join_called = true; end
  end

  class ForceShutdownSpyWorker < WorkerSpy
    attr_reader :raised_error

    def start; end
    def raise(error); @raised_error = error; end
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
