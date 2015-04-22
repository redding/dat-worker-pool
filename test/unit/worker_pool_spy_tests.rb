require 'assert'
require 'dat-worker-pool/worker_pool_spy'

class DatWorkerPool::WorkerPoolSpy

  class UnitTests < Assert::Context
    desc "DatWorkerPool::WorkerPoolSpy"
    setup do
      @work_proc = proc{ 'work' }
      @worker_pool_spy = DatWorkerPool::WorkerPoolSpy.new(&@work_proc)
    end
    subject{ @worker_pool_spy }

    should have_readers :work_proc, :work_items
    should have_readers :start_called, :shutdown_called, :shutdown_timeout
    should have_readers :on_queue_pop_callbacks, :on_queue_push_callbacks
    should have_readers :on_worker_error_callbacks
    should have_readers :on_worker_start_callbacks, :on_worker_shutdown_callbacks
    should have_readers :on_worker_sleep_callbacks, :on_worker_wakeup_callbacks
    should have_readers :before_work_callbacks, :after_work_callbacks
    should have_accessors :worker_available
    should have_imeths :worker_available?, :queue_empty?
    should have_imeths :add_work, :start, :shutdown
    should have_imeths :on_queue_pop, :on_queue_push
    should have_imeths :on_worker_error
    should have_imeths :on_worker_start, :on_worker_shutdown
    should have_imeths :on_worker_sleep, :on_worker_wakeup
    should have_imeths :before_work, :after_work

    should "know its work proc" do
      assert_equal @work_proc, subject.work_proc
    end

    should "have nothing in it's work items by default" do
      assert subject.work_items.empty?
    end

    should "not have a worker available by default" do
      assert_equal false, subject.worker_available
      assert_not subject.worker_available?
    end

    should "return false for start called by default" do
      assert_equal false, subject.start_called
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

    should "know when it's been started" do
      subject.start
      assert_true subject.start_called
    end

    should "know when it's been shutdown and with what timeout" do
      subject.shutdown(10)
      assert_true subject.shutdown_called
      assert_equal 10, subject.shutdown_timeout
    end

    should "allow calling shutdown with no timeout" do
      subject.shutdown
      assert_true subject.shutdown_called
      assert_nil subject.shutdown_timeout
    end

    should "know its queue and worker callbacks" do
      assert_equal [], subject.on_queue_pop_callbacks
      callback = proc{ }
      subject.on_queue_pop(&callback)
      assert_equal [callback], subject.on_queue_pop_callbacks

      assert_equal [], subject.on_queue_push_callbacks
      callback = proc{ }
      subject.on_queue_push(&callback)
      assert_equal [callback], subject.on_queue_push_callbacks

      assert_equal [], subject.on_worker_error_callbacks
      callback = proc{ }
      subject.on_worker_error(&callback)
      assert_equal [callback], subject.on_worker_error_callbacks

      assert_equal [], subject.on_worker_start_callbacks
      callback = proc{ }
      subject.on_worker_start(&callback)
      assert_equal [callback], subject.on_worker_start_callbacks

      assert_equal [], subject.on_worker_shutdown_callbacks
      callback = proc{ }
      subject.on_worker_shutdown(&callback)
      assert_equal [callback], subject.on_worker_shutdown_callbacks

      assert_equal [], subject.on_worker_sleep_callbacks
      callback = proc{ }
      subject.on_worker_sleep(&callback)
      assert_equal [callback], subject.on_worker_sleep_callbacks

      assert_equal [], subject.on_worker_wakeup_callbacks
      callback = proc{ }
      subject.on_worker_wakeup(&callback)
      assert_equal [callback], subject.on_worker_wakeup_callbacks

      assert_equal [], subject.before_work_callbacks
      callback = proc{ }
      subject.before_work(&callback)
      assert_equal [callback], subject.before_work_callbacks

      assert_equal [], subject.after_work_callbacks
      callback = proc{ }
      subject.after_work(&callback)
      assert_equal [callback], subject.after_work_callbacks
    end

  end

end
