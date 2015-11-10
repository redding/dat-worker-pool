require 'assert'
require 'dat-worker-pool'

require 'system_timer'
require 'dat-worker-pool/queue'
require 'dat-worker-pool/runner'

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

  class InitSetupTests < UnitTests
    desc "when init"
    setup do
      @num_workers = Factory.integer(4)
      @logger      = NullLogger.new
      @queue       = TestQueue.new

      @runner_spy = RunnerSpy.new
      Assert.stub(DatWorkerPool::Runner, :new) do |args|
        @runner_spy.args = args
        @runner_spy
      end

      @options = {
        :do_work_proc => proc{ },
        :num_workers  => @num_workers,
        :logger       => @logger,
        :queue        => @queue
      }
    end
    subject{ @worker_pool }

  end

  class InitTests < InitSetupTests
    desc "when init"
    setup do
      @worker_pool = @worker_pool_class.new(@options)
    end
    teardown do
      # TODO - remove once a worker class can be passed
      DefaultWorker.on_start_callbacks.clear
      DefaultWorker.on_shutdown_callbacks.clear
      DefaultWorker.on_sleep_callbacks.clear
      DefaultWorker.on_wakeup_callbacks.clear
      DefaultWorker.on_error_callbacks.clear
      DefaultWorker.before_work_callbacks.clear
      DefaultWorker.after_work_callbacks.clear
    end
    subject{ @worker_pool }

    should have_readers :logger, :queue
    should have_imeths :start, :shutdown, :add_work
    should have_imeths :num_workers, :waiting, :worker_available?
    should have_imeths :on_worker_start_callbacks, :on_worker_shutdown_callbacks
    should have_imeths :on_worker_sleep_callbacks, :on_worker_wakeup_callbacks
    should have_imeths :on_worker_error_callbacks
    should have_imeths :before_work_callbacks, :after_work_callbacks
    should have_imeths :on_worker_start, :on_worker_shutdown
    should have_imeths :on_worker_sleep, :on_worker_wakeup
    should have_imeths :on_worker_error
    should have_imeths :before_work, :after_work

    should "know its attributes" do
      assert_equal @logger, subject.logger
      assert_equal @queue,  subject.queue
    end

    should "build a runner" do
      exp = {
        :num_workers  => @num_workers,
        :queue        => @queue,
        :worker_class => DefaultWorker,
        :do_work_proc => @options[:do_work_proc]
      }
      assert_equal exp, @runner_spy.args
    end

    should "default its attributes" do
      worker_pool = @worker_pool_class.new
      assert_instance_of NullLogger, worker_pool.logger
      assert_instance_of DatWorkerPool::DefaultQueue, worker_pool.queue

      assert_equal DEFAULT_NUM_WORKERS, @runner_spy.args[:num_workers]
    end

    should "start its runner when its started" do
      assert_false @runner_spy.start_called
      subject.start
      assert_true @runner_spy.start_called
    end

    should "shutdown its runner when its shutdown" do
      assert_false @runner_spy.shutdown_called
      subject.shutdown
      assert_true @runner_spy.shutdown_called
      assert_nil @runner_spy.shutdown_timeout

      timeout = Factory.integer
      subject.shutdown(timeout)
      assert_equal timeout, @runner_spy.shutdown_timeout
    end

    should "demeter its runner" do
      assert_equal @runner_spy.num_workers,           subject.num_workers
      assert_equal @runner_spy.workers_waiting_count, subject.waiting
    end

    should "raise an argument error if given an invalid number of workers" do
      assert_raises(ArgumentError) do
        @worker_pool_class.new({
          :num_workers  => [0, (Factory.integer * -1)].choice,
          :do_work_proc => proc{ }
        })
      end
    end

    # TODO - remove once a worker class can be passed
    should "demeter its worker callbacks" do
      callback = proc{ Factory.string }

      subject.on_worker_start(&callback)
      assert_equal callback, DefaultWorker.on_start_callbacks.last
      exp = DefaultWorker.on_start_callbacks
      assert_equal exp, subject.on_worker_start_callbacks

      subject.on_worker_shutdown(&callback)
      assert_equal callback, DefaultWorker.on_shutdown_callbacks.last
      exp = DefaultWorker.on_shutdown_callbacks
      assert_equal exp, subject.on_worker_shutdown_callbacks

      subject.on_worker_sleep(&callback)
      assert_equal callback, DefaultWorker.on_sleep_callbacks.last
      exp = DefaultWorker.on_sleep_callbacks
      assert_equal exp, subject.on_worker_sleep_callbacks

      subject.on_worker_wakeup(&callback)
      assert_equal callback, DefaultWorker.on_wakeup_callbacks.last
      exp = DefaultWorker.on_wakeup_callbacks
      assert_equal exp, subject.on_worker_wakeup_callbacks

      subject.on_worker_error(&callback)
      assert_equal callback, DefaultWorker.on_error_callbacks.last
      exp = DefaultWorker.on_error_callbacks
      assert_equal exp, subject.on_worker_error_callbacks

      subject.before_work(&callback)
      assert_equal callback, DefaultWorker.before_work_callbacks.last
      exp = DefaultWorker.before_work_callbacks
      assert_equal exp, subject.before_work_callbacks

      subject.after_work(&callback)
      assert_equal callback, DefaultWorker.after_work_callbacks.last
      exp = DefaultWorker.after_work_callbacks
      assert_equal exp, subject.after_work_callbacks
    end

  end

  class StartedTests < InitTests
    desc "and started"
    setup do
      @worker_pool.start
    end

    should "push work onto its queue using `add_work`" do
      work_item = Factory.string
      subject.add_work(work_item)
      assert_equal [work_item], @queue.pushed_work_items
    end

    should "not push `nil` work onto its queue using `add_work`" do
      subject.add_work(nil)
      assert_equal [], @queue.pushed_work_items
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

  class TestQueue
    include DatWorkerPool::Queue

    attr_reader :pushed_work_items

    def initialize
      @pushed_work_items = []
    end

    private

    def push!(work_item)
      @pushed_work_items << work_item
    end
  end

  class RunnerSpy < DatWorkerPool::Runner
    attr_accessor :args
    attr_reader :start_called, :shutdown_timeout, :shutdown_called

    def initialize
      super({})
      @start_called     = false
      @shutdown_timeout = nil
      @shutdown_called  = false
    end

    def start
      @args[:queue].start
      @start_called = true
    end

    def shutdown(timeout)
      @args[:queue].shutdown
      @shutdown_timeout = timeout
      @shutdown_called  = true
    end
  end

end
