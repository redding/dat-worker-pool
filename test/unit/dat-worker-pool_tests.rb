require 'assert'
require 'dat-worker-pool'

require 'system_timer'
require 'dat-worker-pool/queue'
require 'dat-worker-pool/runner'
require 'dat-worker-pool/worker'

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
      @num_workers   = Factory.integer(4)
      @logger        = NullLogger.new
      @queue         = TestQueue.new
      @worker_params = { Factory.string => Factory.string }

      @runner_spy = RunnerSpy.new
      Assert.stub(DatWorkerPool::Runner, :new) do |args|
        @runner_spy.args = args
        @runner_spy
      end

      @worker_class = Class.new do
        include DatWorkerPool::Worker
        def work!(work_item); end
      end
      @options = {
        :num_workers   => @num_workers,
        :logger        => @logger,
        :queue         => @queue,
        :worker_params => @worker_params
      }
    end
    subject{ @worker_pool }

  end

  class InitTests < InitSetupTests
    desc "when init"
    setup do
      @worker_pool = @worker_pool_class.new(@worker_class, @options)
    end
    subject{ @worker_pool }

    should have_readers :logger, :queue
    should have_imeths :start, :shutdown, :add_work
    should have_imeths :available_worker_count, :worker_available?

    should "know its attributes" do
      assert_equal @logger, subject.logger
      assert_equal @queue,  subject.queue
    end

    should "build a runner" do
      exp = {
        :num_workers   => @num_workers,
        :queue         => @queue,
        :worker_class  => @worker_class,
        :worker_params => @worker_params
      }
      assert_equal exp, @runner_spy.args
    end

    should "default its attributes" do
      worker_pool = @worker_pool_class.new(@worker_class)
      assert_instance_of NullLogger, worker_pool.logger
      assert_instance_of DatWorkerPool::DefaultQueue, worker_pool.queue

      assert_equal DEFAULT_NUM_WORKERS, @runner_spy.args[:num_workers]
      assert_nil @runner_spy.args[:worker_params]
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
      exp = "test/unit/dat-worker-pool_tests.rb"
      assert_match exp, @runner_spy.shutdown_backtrace.first

      timeout = Factory.integer
      subject.shutdown(timeout)
      assert_equal timeout, @runner_spy.shutdown_timeout
    end

    should "demeter its runner" do
      assert_equal @runner_spy.available_worker_count, subject.available_worker_count
      assert_equal @runner_spy.worker_available?,      subject.worker_available?
    end

    should "raise an argument error if given an invalid worker class" do
      assert_raises(ArgumentError){ @worker_pool_class.new(Module.new) }
      assert_raises(ArgumentError){ @worker_pool_class.new(Class.new) }
    end

    should "raise an argument error if given an invalid number of workers" do
      assert_raises(ArgumentError) do
        @worker_pool_class.new(@worker_class, {
          :num_workers  => [0, (Factory.integer * -1)].choice
        })
      end
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
    attr_reader :start_called, :shutdown_called
    attr_reader :shutdown_timeout, :shutdown_backtrace

    def initialize
      super({})
      @start_called       = false
      @shutdown_called    = false
      @shutdown_timeout   = nil
      @shutdown_backtrace = nil
    end

    def start
      @args[:queue].dwp_start
      @start_called = true
    end

    def shutdown(timeout, backtrace)
      @args[:queue].dwp_shutdown
      @shutdown_called    = true
      @shutdown_timeout   = timeout
      @shutdown_backtrace = backtrace
    end
  end

end
