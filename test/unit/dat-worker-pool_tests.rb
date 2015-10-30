require 'assert'
require 'dat-worker-pool'

class DatWorkerPool

  class UnitTests < Assert::Context
    desc "DatWorkerPool"
    setup do
      @work_pool = DatWorkerPool.new{ }
    end
    subject{ @work_pool }

    should have_readers :logger, :queue
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

    should "know its attributes" do
      assert_instance_of ::Logger, subject.logger
      assert_instance_of Queue, subject.queue
    end

    should "default its worker callbacks" do
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

    should "demeter its queue's callbacks" do
      callback = proc{ }
      subject.on_queue_pop(&callback)
      assert_equal [callback], subject.on_queue_pop_callbacks
      callback = proc{ }
      subject.on_queue_push(&callback)
      assert_equal [callback], subject.on_queue_push_callbacks
    end

  end

  class WorkerBehaviorTests < UnitTests
    desc "workers"
    setup do
      @work_pool = DatWorkerPool.new(2, true){ |work| sleep(work) }
      @work_pool.start
    end

    should "spawn and wait until work is available" do
      # the workers should be spawned and waiting
      assert_equal 2,    @work_pool.waiting
      assert_equal true, @work_pool.worker_available?

      # only one worker should be waiting
      @work_pool.add_work 5
      assert_equal 1,    @work_pool.waiting
      assert_equal true, @work_pool.worker_available?

      # neither worker should be waiting now
      @work_pool.add_work 5
      assert_equal 0,     @work_pool.waiting
      assert_equal false, @work_pool.worker_available?
    end

    should "go back to waiting when they finish working" do
      assert_equal 2, @work_pool.waiting
      @work_pool.add_work 1
      assert_equal 1, @work_pool.waiting

      sleep 1 # allow the worker to run

      assert_equal 2, @work_pool.waiting
    end

  end

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

      @work_pool = DatWorkerPool.new(1) do |work|
        raise work if work == 'error'
      end
      @work_pool.on_worker_error{ @error_called = true }
      @work_pool.on_worker_start{ @start_called = true }
      @work_pool.on_worker_shutdown{ @shutdown_called = true }
      @work_pool.on_worker_sleep{ @sleep_called = true }
      @work_pool.on_worker_wakeup{ @wakeup_called = true }
      @work_pool.before_work{ @before_work_called = true }
      @work_pool.after_work{ @after_work_called = true }
    end
    subject{ @work_pool }

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

  class AddWorkWithNoWorkersTests < UnitTests
    setup do
      @work_pool = DatWorkerPool.new(0, 0){ |work| }
    end

    should "return whether or not the queue is empty" do
      assert_equal true, @work_pool.queue_empty?
      @work_pool.add_work 'test'
      assert_equal false, @work_pool.queue_empty?
    end

  end

  class AddWorkAndProcessItTests < UnitTests
    desc "add_work and process"
    setup do
      @result = nil
      @work_pool = DatWorkerPool.new(1){|work| @result = (2 / work) }
      @work_pool.start
    end

    should "have added the work and processed it by calling the passed block" do
      subject.add_work 2
      sleep 0.1 # ensure worker thread get's a chance to run
      assert_equal 1, @result
    end

    should "swallow exceptions, so workers don't end unexpectedly" do
      subject.add_work 0
      worker = subject.instance_variable_get("@workers").first
      sleep 0.1

      assert_equal 1, subject.waiting
      assert worker.instance_variable_get("@thread").alive?
    end

  end

  class StartTests < UnitTests
    desc "start"
    setup do
      @work_pool = DatWorkerPool.new(1, 2){ |work| sleep(work) }
    end
    subject{ @work_pool }

    should "start its queue" do
      assert_false subject.queue.shutdown?
      subject.start
      assert_false subject.queue.shutdown?
      subject.shutdown
      assert_true subject.queue.shutdown?
      subject.start
      assert_false subject.queue.shutdown?
    end

  end

  class ShutdownTests < UnitTests
    desc "shutdown"
    setup do
      @mutex = Mutex.new
      @finished = []
      @work_pool = DatWorkerPool.new(2, true) do |work|
        sleep 1
        @mutex.synchronize{ @finished << work }
      end
      @work_pool.start
      @work_pool.add_work 'a'
      @work_pool.add_work 'b'
      @work_pool.add_work 'c'
    end

    should "allow any work that has been picked up to be processed" do
      # make sure the workers haven't processed any work
      assert_equal [], @finished
      subject.shutdown(5)

      # NOTE, the last work shouldn't have been processed, as it wasn't
      # picked up by a worker
      assert_includes     'a', @finished
      assert_includes     'b', @finished
      assert_not_includes 'c', @finished

      assert_equal 0, subject.waiting
      assert_includes 'c', subject.work_items
    end

    should "allow jobs to finish by not providing a shutdown timeout" do
      assert_equal [], @finished
      subject.shutdown
      assert_includes 'a', @finished
      assert_includes 'b', @finished
    end

    should "reraise shutdown errors in debug mode if workers take to long to finish" do
      assert_raises(DatWorkerPool::ShutdownError) do
        subject.shutdown(0.1)
      end
    end

  end

  class ForcedShutdownTests < UnitTests
    desc "forced shutdown"
    setup do
      @mutex = Mutex.new
      @finished = []
      @max_workers = 2
      # don't put leave the worker pool in debug mode
      @work_pool = DatWorkerPool.new(@max_workers, false) do |work|
        begin
          sleep 1
        rescue ShutdownError => error
          @mutex.synchronize{ @finished << error }
          raise error # re-raise it otherwise worker won't shutdown
        end
      end
      @work_pool.start
      @work_pool.add_work 'a'
      @work_pool.add_work 'b'
      @work_pool.add_work 'c'
    end

    should "force workers to shutdown if they take to long to finish" do
      # make sure the workers haven't processed any work
      assert_equal [], @finished
      subject.shutdown(0.1)
      assert_equal @max_workers, @finished.size
      @finished.each do |error|
        assert_instance_of DatWorkerPool::ShutdownError, error
      end
    end

  end

end
