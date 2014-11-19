require 'assert'
require 'dat-worker-pool/worker_pool_spy'

class DatWorkerPool::WorkerPoolSpy

  class UnitTests < Assert::Context
    desc "DatWorkerPool::WorkerPoolSpy"
    setup do
      @worker_pool_spy = DatWorkerPool::WorkerPoolSpy.new
    end
    subject{ @worker_pool_spy }

    should have_readers :work_items
    should have_readers :shutdown_called, :shutdown_timeout
    should have_accessors :worker_available
    should have_imeths :worker_available?, :queue_empty?
    should have_imeths :add_work, :shutdown

    should "have nothing in it's work items by default" do
      assert subject.work_items.empty?
    end

    should "not have a worker available by default" do
      assert_equal false, subject.worker_available
      assert_not subject.worker_available?
    end

    should "return false for shutdown called by default" do
      assert_equal false, subject.shutdown_called
    end

    should "return `nil` for shutdown timeout by default" do
      assert_nil subject.shutdown_timeout
    end

    should "allow setting whether a worker is available" do
      subject.worker_available = true
      assert_equal true, subject.worker_available
      assert subject.worker_available?
    end

    should "allow adding work to the work items with #add_work" do
      subject.add_work 'work'
      assert_equal 1, subject.work_items.size
      assert_includes 'work', subject.work_items
    end

    should "not add `nil` work to the work items with #add_work" do
      subject.add_work nil
      assert_equal 0, subject.work_items.size
    end

    should "return whether the work items is empty with #queue_empty?" do
      assert_equal true, subject.queue_empty?
      subject.add_work 'work'
      assert_equal false, subject.queue_empty?
    end

    should "know when it's been shutdown and with what timeout" do
      subject.shutdown(10)
      assert subject.shutdown_called
      assert_equal 10, subject.shutdown_timeout
    end

    should "allow calling shutdown with no timeout" do
      subject.shutdown
      assert_true subject.shutdown_called
      assert_nil subject.shutdown_timeout
    end

  end

end
