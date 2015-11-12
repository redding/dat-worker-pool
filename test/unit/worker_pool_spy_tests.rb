require 'assert'
require 'dat-worker-pool/worker_pool_spy'

require 'dat-worker-pool'
require 'dat-worker-pool/default_queue'
require 'dat-worker-pool/worker'

class DatWorkerPool::WorkerPoolSpy

  class UnitTests < Assert::Context
    desc "DatWorkerPool::WorkerPoolSpy"
    setup do
      @spy_class = DatWorkerPool::WorkerPoolSpy
    end
    subject{ @spy_class }

  end

  class InitTests < UnitTests
    desc "when init"
    setup do
      @worker_class = Class.new{ include DatWorkerPool::Worker }
      @options = {
        :num_workers   => Factory.integer,
        :logger        => DatWorkerPool::NullLogger.new,
        :queue         => DatWorkerPool::DefaultQueue.new,
        :worker_params => { Factory.string => Factory.string }
      }

      @worker_pool_spy = @spy_class.new(@worker_class, @options)
    end
    subject{ @worker_pool_spy }

    should have_readers :logger, :queue
    should have_readers :options, :num_workers, :worker_class, :worker_params
    should have_readers :work_items
    should have_readers :start_called, :shutdown_called, :shutdown_timeout
    should have_accessors :worker_available

    should "know its attributes" do
      assert_equal @worker_class,            subject.worker_class
      assert_equal @options,                 subject.options
      assert_equal @options[:num_workers],   subject.num_workers
      assert_equal @options[:logger],        subject.logger
      assert_equal @options[:queue],         subject.queue
      assert_equal @options[:worker_params], subject.worker_params

      assert_false subject.worker_available?
      assert_equal [], subject.work_items
      assert_false subject.start_called
      assert_false subject.shutdown_called
      assert_nil subject.shutdown_timeout
    end

    should "allow setting whether a worker is available" do
      subject.worker_available = true
      assert_true subject.worker_available?
      subject.worker_available = false
      assert_false subject.worker_available?
    end

    should "know if it's been started" do
      assert_false subject.start_called
      subject.start
      assert_true subject.start_called
    end

    should "know if it's been shutdown" do
      assert_false subject.shutdown_called
      subject.shutdown
      assert_true subject.shutdown_called
      assert_nil subject.shutdown_timeout
    end

    should "know if it's been shutdown with a timeout" do
      timeout = Factory.integer
      subject.shutdown(timeout)
      assert_equal timeout, subject.shutdown_timeout
    end

    should "allow adding work" do
      work_item = Factory.string
      subject.add_work(work_item)
      assert_equal 1, subject.work_items.size
      assert_includes work_item, subject.work_items
    end

    should "not allow adding `nil` work" do
      subject.add_work(nil)
      assert_equal 0, subject.work_items.size
    end

  end

end
