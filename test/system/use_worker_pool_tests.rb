require 'assert'
require 'dat-worker-pool'

require 'dat-worker-pool/worker'

class DatWorkerPool

  class SystemTests < Assert::Context
    desc "DatWorkerPool"
    subject{ @worker_pool }

  end

  class UseWorkerPoolTests < SystemTests
    setup do
      @worker_class = Class.new do
        include DatWorkerPool::Worker
        def work!(number); params[:results].push(number * 100); end
      end
      @results = LockedArray.new

      @worker_pool = DatWorkerPool.new(@worker_class, {
        :num_workers   => 2,
        :logger        => TEST_LOGGER,
        :worker_params => { :results => @results }
      })
      @worker_pool.start
    end

    should "be able to add work, have it processed and stop the pool" do
      subject.add_work 1
      subject.add_work 5
      subject.add_work 2
      subject.add_work 4
      subject.add_work 3

      sleep 0.1 # allow the worker threads to run
      subject.shutdown(1)

      assert_includes 100, @results.values
      assert_includes 200, @results.values
      assert_includes 300, @results.values
      assert_includes 400, @results.values
      assert_includes 500, @results.values
    end

  end

  class WorkerBehaviorTests < SystemTests
    desc "workers"
    setup do
      @worker_class = Class.new do
        include DatWorkerPool::Worker
        def work!(seconds); sleep(seconds); end
      end
      @worker_pool = DatWorkerPool.new(@worker_class, {
        :num_workers  => 2,
        :logger       => TEST_LOGGER
      })
      @worker_pool.start
    end

    should "spawn and wait until work is available" do
      # the workers should be spawned and available
      assert_equal 2, subject.available_worker_count
      assert_true subject.worker_available?

      # only one worker should be available
      subject.add_work 5
      assert_equal 1, subject.available_worker_count
      assert_true subject.worker_available?

      # neither worker should be available now
      subject.add_work 5
      assert_equal 0, subject.available_worker_count
      assert_false subject.worker_available?
    end

    should "go back to waiting when they finish working" do
      assert_equal 2, subject.available_worker_count
      assert_true subject.worker_available?
      subject.add_work 1
      subject.add_work 1
      assert_equal 0, subject.available_worker_count
      assert_false subject.worker_available?

      sleep 1 # allow the workers to run
      assert_equal 2, subject.available_worker_count
      assert_true subject.worker_available?
    end

  end

  class WorkerCallbackTests < SystemTests
    setup do
      @callbacks_called = {}

      @worker_class = Class.new do
        include DatWorkerPool::Worker

        on_start{ params[:callbacks_called][:on_start] = true }
        on_shutdown{ params[:callbacks_called][:on_shutdown] = true }

        on_available{ params[:callbacks_called][:on_available] = true }
        on_unavailable{ params[:callbacks_called][:on_unavailable] = true }

        on_error{ |e, wi| params[:callbacks_called][:on_error] = true }

        before_work{ |wi| params[:callbacks_called][:before_work] = true }
        after_work{ |wi| params[:callbacks_called][:after_work] = true }

        def work!(work_item); raise work_item if work_item == 'error'; end
      end

      @worker_pool = DatWorkerPool.new(@worker_class, {
        :logger        => TEST_LOGGER,
        :worker_params => { :callbacks_called => @callbacks_called }
      })
    end
    subject{ @worker_pool }

    should "call the worker callbacks as workers wait or wakeup" do
      assert_nil @callbacks_called[:on_start]
      assert_nil @callbacks_called[:on_available]
      subject.start
      assert_true @callbacks_called[:on_start]

      assert_nil @callbacks_called[:on_unavailable]
      assert_nil @callbacks_called[:before_work]
      assert_nil @callbacks_called[:after_work]
      subject.add_work Factory.string
      assert_true @callbacks_called[:on_unavailable]
      assert_true @callbacks_called[:before_work]
      assert_true @callbacks_called[:after_work]
      assert_true @callbacks_called[:on_available]

      @callbacks_called.delete(:on_unavailable)
      @callbacks_called.delete(:before_work)
      @callbacks_called.delete(:after_work)
      @callbacks_called.delete(:on_available)
      assert_nil @callbacks_called[:on_unavailable]
      assert_nil @callbacks_called[:before_work]
      assert_nil @callbacks_called[:on_error]
      assert_nil @callbacks_called[:after_work]
      assert_nil @callbacks_called[:on_available]
      subject.add_work 'error'
      assert_true @callbacks_called[:on_unavailable]
      assert_true @callbacks_called[:before_work]
      assert_true @callbacks_called[:on_error]
      assert_nil @callbacks_called[:after_work]
      assert_true @callbacks_called[:on_available]

      @callbacks_called.delete(:on_unavailable)
      assert_nil @callbacks_called[:on_shutdown]
      subject.shutdown
      assert_true @callbacks_called[:on_shutdown]
    end

  end

  class ShutdownSystemTests < SystemTests
    desc "shutdown"
    setup do
      @worker_class = Class.new do
        include DatWorkerPool::Worker
        def work!(work_item); sleep 1; params[:finished].push(work_item); end
      end
      @finished = LockedArray.new

      @worker_pool = DatWorkerPool.new(@worker_class, {
        :num_workers   => 2,
        :logger        => TEST_LOGGER,
        :worker_params => { :finished => @finished }
      })
      @worker_pool.start
      @worker_pool.add_work 'a'
      @worker_pool.add_work 'b'
      @worker_pool.add_work 'c'
    end

    should "allow any work that has been picked up to be processed" do
      # make sure the workers haven't processed any work
      assert_equal [], @finished.values
      subject.shutdown(5)

      # NOTE, the last work shouldn't have been processed, as it wasn't
      # picked up by a worker
      assert_includes     'a', @finished.values
      assert_includes     'b', @finished.values
      assert_not_includes 'c', @finished.values

      assert_equal 0, subject.available_worker_count
      assert_includes 'c', subject.queue.work_items
    end

    should "allow jobs to finish by not providing a shutdown timeout" do
      assert_equal [], @finished.values
      subject.shutdown
      assert_includes 'a', @finished.values
      assert_includes 'b', @finished.values
    end

  end

  class ForcedShutdownSystemTests < SystemTests
    desc "forced shutdown"
    setup do
      @worker_class = Class.new do
        include DatWorkerPool::Worker

        on_error{ |e, wi| params[:finished].push(e) }

        def work!(work_item); sleep 1; end
      end
      @num_workers = 2
      @finished    = LockedArray.new

      @worker_pool = DatWorkerPool.new(@worker_class, {
        :num_workers   => @num_workers,
        :logger        => TEST_LOGGER,
        :worker_params => { :finished => @finished }
      })
      @worker_pool.start
      @worker_pool.add_work 'a'
      @worker_pool.add_work 'b'
      @worker_pool.add_work 'c'
    end

    should "force workers to shutdown if they take too long to finish" do
      # make sure the workers haven't processed any work
      assert_equal [], @finished.values
      subject.shutdown(0.1)
      assert_equal @num_workers, @finished.values.size
      @finished.values.each do |error|
        assert_instance_of DatWorkerPool::ShutdownError, error
      end
    end

  end

  class LockedArray
    def initialize
      @mutex = Mutex.new
      @array = []
    end

    def push(value)
      @mutex.synchronize{ @array.push(value) }
    end

    def values
      @mutex.synchronize{ @array }
    end
  end

end
