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
      @worker_class  = TestWorker
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
    teardown do
      subject.shutdown(0) rescue false
    end
    subject{ @runner }

    should have_readers :num_workers, :worker_class, :worker_params
    should have_readers :queue, :workers
    should have_imeths :start, :shutdown
    should have_imeths :workers_waiting_count
    should have_imeths :increment_worker_waiting, :decrement_worker_waiting
    should have_imeths :add_worker, :remove_worker

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

    should "build workers when its started" do
      passed_runner = nil
      passed_queue  = nil
      workers = @num_workers.times.map{ @worker_class.new(@runner, @queue) }
      Assert.stub(@worker_class, :new) do |runner, queue|
        passed_runner = runner
        passed_queue  = queue
        workers.pop
      end

      subject.start

      assert_equal subject, passed_runner
      assert_equal @queue,  passed_queue
      subject.workers.each do |worker|
        assert_true worker.dwp_running?
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

    should "add a worker to its workers using `add_worker`" do
      worker = @worker_class.new(@runner, @queue)

      assert_false @mutex_spy.synchronize_called
      subject.add_worker(worker)
      assert_true @mutex_spy.synchronize_called
      assert_includes worker, subject.workers

      subject.workers.delete(worker)
    end

    should "remove a worker from its workers using `remove_worker`" do
      worker = subject.workers.choice

      assert_false @mutex_spy.synchronize_called
      subject.remove_worker(worker)
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

      @options[:worker_class] = ShutdownSpyWorker
      @runner = @runner_class.new(@options)
      @runner.start
      # we need a reference to the workers, the runners workers will get removed
      # as they shutdown
      @running_workers = @runner.workers.dup
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
      @running_workers.each do |worker|
        assert_false worker.dwp_shutdown?
      end
      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      @running_workers.each do |worker|
        assert_true worker.dwp_shutdown?
      end
    end

    should "shutdown its queue" do
      assert_false @queue.shutdown?
      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      assert_true @queue.shutdown?
    end

    should "join its workers waiting for them to finish" do
      @running_workers.each do |worker|
        assert_false worker.join_called
      end
      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      @running_workers.each do |worker|
        assert_true worker.join_called
      end
    end

    should "join all workers even if one raises an error when joined" do
      @running_workers.choice.join_error = Factory.exception
      subject.shutdown(Factory.boolean ? Factory.integer : nil)
      @running_workers.each do |worker|
        assert_true worker.join_called
      end
      assert_equal [], subject.workers
    end

    should "force its workers to shutdown if a timeout error occurs" do
      Assert.stub(OptionalTimeout, :new){ raise TimeoutInterruptError }
      subject.shutdown(Factory.integer)

      @running_workers.each do |worker|
        assert_instance_of DatWorkerPool::ShutdownError, worker.raised_error
        assert_true worker.join_called
      end
      assert_equal [], subject.workers
    end

    should "force its workers to shutdown if a non-timeout error occurs" do
      queue_exception = Factory.exception
      Assert.stub(@queue, :shutdown){ raise queue_exception }

      caught_exception = nil
      begin
        subject.shutdown(Factory.integer)
      rescue StandardError => caught_exception
      end
      assert_same queue_exception, caught_exception

      @running_workers.each do |worker|
        assert_instance_of DatWorkerPool::ShutdownError, worker.raised_error
        assert_true worker.join_called
      end
      assert_equal [], subject.workers
    end

    should "force shutdown all of its workers even if one raises an error when joining" do
      Assert.stub(OptionalTimeout, :new){ raise TimeoutInterruptError }
      error_class = Factory.boolean ? DatWorkerPool::ShutdownError : RuntimeError
      @running_workers.choice.join_error = Factory.exception(error_class)
      subject.shutdown(Factory.boolean ? Factory.integer : nil)

      @running_workers.each do |worker|
        assert_instance_of DatWorkerPool::ShutdownError, worker.raised_error
        assert_true worker.join_called
      end
      assert_equal [], subject.workers
    end

  end

  class TestWorker
    include DatWorkerPool::Worker
  end

  class ShutdownSpyWorker < TestWorker
    attr_reader :join_called
    attr_accessor :join_error, :raised_error

    # this pauses the shutdown so we can test that join or raise are called
    # depending if we are doing a standard or forced shutdown; otherwise the
    # worker threads can exit before join or raise ever gets called on them and
    # then there is nothing to test
    on_shutdown{ wait_for_join_or_raise }

    def initialize(*args)
      super
      @mutex        = Mutex.new
      @cond_var     = ConditionVariable.new
      @join_called  = false
      @join_error   = nil
      @raised_error = nil
    end

    def dwp_join(*args)
      @join_called = true
      raise @join_error if @join_error
      @mutex.synchronize{ @cond_var.broadcast }
    end

    def dwp_raise(error)
      @raised_error = error
    end

    private

    def wait_for_join_or_raise
      @mutex.synchronize{ @cond_var.wait(@mutex) }
    end
  end

end
