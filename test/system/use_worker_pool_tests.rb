require 'assert'
require 'dat-worker-pool'

class DatWorkerPool

  class SystemTests < Assert::Context
    desc "DatWorkerPool"
    subject{ @work_pool }

  end

  class UseWorkerPoolTests < SystemTests
    setup do
      @mutex = Mutex.new
      @results = []
      @work_pool = DatWorkerPool.new({
        :num_workers  => 2,
        :logger       => TEST_LOGGER,
        :do_work_proc => proc do |work|
          @mutex.synchronize{ @results << (work * 100) }
        end
      })
      @work_pool.start
    end

    should "be able to add work, have it processed and stop the pool" do
      @work_pool.add_work 1
      @work_pool.add_work 5
      @work_pool.add_work 2
      @work_pool.add_work 4
      @work_pool.add_work 3

      sleep 0.1 # allow the worker threads to run

      @work_pool.shutdown(1)

      assert_includes 100, @results
      assert_includes 200, @results
      assert_includes 300, @results
      assert_includes 400, @results
      assert_includes 500, @results
    end

  end

  class WorkerBehaviorTests < SystemTests
    desc "workers"
    setup do
      @work_pool = DatWorkerPool.new({
        :num_workers  => 2,
        :logger       => TEST_LOGGER,
        :do_work_proc => proc{ |work| sleep(work) }
      })
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

  class WorkerCallbackTests < SystemTests
    setup do
      @error_called       = false
      @start_called       = false
      @shutdown_called    = false
      @sleep_called       = false
      @wakeup_called      = false
      @before_work_called = false
      @after_work_called  = false

      @worker_pool = DatWorkerPool.new({
        :do_work_proc => proc{ |work| raise work if work == 'error' }
      })

      @worker_pool.on_worker_error{ @error_called = true }
      @worker_pool.on_worker_start{ @start_called = true }
      @worker_pool.on_worker_shutdown{ @shutdown_called = true }
      @worker_pool.on_worker_sleep{ @sleep_called = true }
      @worker_pool.on_worker_wakeup{ @wakeup_called = true }
      @worker_pool.before_work{ @before_work_called = true }
      @worker_pool.after_work{ @after_work_called = true }
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

  class ShutdownSystemTests < SystemTests
    desc "shutdown"
    setup do
      @mutex = Mutex.new
      @finished = []
      @work_pool = DatWorkerPool.new({
        :num_workers  => 2,
        :logger       => TEST_LOGGER,
        :do_work_proc => proc do |work|
          sleep 1
          @mutex.synchronize{ @finished << work }
        end
      })
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
      assert_includes 'c', subject.queue.work_items
    end

    should "allow jobs to finish by not providing a shutdown timeout" do
      assert_equal [], @finished
      subject.shutdown
      assert_includes 'a', @finished
      assert_includes 'b', @finished
    end

  end

  class ForcedShutdownSystemTests < SystemTests
    desc "forced shutdown"
    setup do
      @mutex = Mutex.new
      @finished = []
      @num_workers = 2
      # don't put leave the worker pool in debug mode
      @work_pool = DatWorkerPool.new({
        :num_workers  => @num_workers,
        :logger       => TEST_LOGGER,
        :do_work_proc => proc do |work|
          begin
            sleep 1
          rescue ShutdownError => error
            @mutex.synchronize{ @finished << error }
            raise error # re-raise it otherwise worker won't shutdown
          end
        end
      })
      @work_pool.start
      @work_pool.add_work 'a'
      @work_pool.add_work 'b'
      @work_pool.add_work 'c'
    end

    should "force workers to shutdown if they take to long to finish" do
      # make sure the workers haven't processed any work
      assert_equal [], @finished
      subject.shutdown(0.1)
      assert_equal @num_workers, @finished.size
      @finished.each do |error|
        assert_instance_of DatWorkerPool::ShutdownError, error
      end
    end

  end

end
