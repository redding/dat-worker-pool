require 'assert'
require 'dat-worker-pool'

require 'timeout'
require 'dat-worker-pool/locked_object'
require 'dat-worker-pool/worker'

class DatWorkerPool

  class SystemTests < Assert::Context
    desc "DatWorkerPool"
    setup do
      # at least 2 workers, up to 4
      @num_workers   = Factory.integer(3) + 1
      @mutex         = Mutex.new
      @cond_var      = ConditionVariable.new
      @worker_params = {
        :mutex    => @mutex,
        :cond_var => @cond_var
      }
    end
    subject{ @worker_pool }

    # this could loop forever so ensure it doesn't by using a timeout; use
    # timeout instead of system timer because system timer is paranoid about a
    # deadlock even though its intended to prevent the deadlock because it times
    # out the block
    def wait_for_workers(&block)
      Timeout.timeout(1) do
        @mutex.synchronize{ @cond_var.wait(@mutex) } while !block.call
      end
    end

    def wait_for_workers_to_become_available
      wait_for_workers{ @worker_pool.available_worker_count == @num_workers }
    end

    def wait_for_workers_to_become_unavailable
      wait_for_workers{ @worker_pool.available_worker_count == 0 }
    end

    def wait_for_a_worker_to_become_available
      wait_for_workers{ @worker_pool.available_worker_count != 0 }
    end

    def wait_for_a_worker_to_become_unavailable
      wait_for_workers{ @worker_pool.available_worker_count != @num_workers }
    end

  end

  class StartAddProcessAndShutdownTests < SystemTests
    setup do
      @worker_class = Class.new do
        include SystemTestWorker
        def work!(number)
          params[:results].push(number * 100)
          signal_test_suite_thread
        end
      end

      # at least 5 work items, up to 10
      @work_items = (Factory.integer(5) + 5).times.map{ Factory.integer(10) }
      @results    = LockedArray.new

      @worker_pool = DatWorkerPool.new(@worker_class, {
        :num_workers   => @num_workers,
        :logger        => TEST_LOGGER,
        :worker_params => @worker_params.merge(:results => @results)
      })
    end

    should "be able to start, add work, process it and shutdown" do
      subject.start
      @work_items.each{ |work_item| subject.add_work(work_item) }

      wait_for_workers{ @results.size == @work_items.size }
      subject.shutdown(0)

      exp = @work_items.map{ |number| number * 100 }
      assert_equal exp, @results.values
    end

  end

  class WorkerAvailabilityTests < SystemTests
    setup do
      @worker_class = Class.new do
        include SystemTestWorker
        on_available{   signal_test_suite_thread }
        on_unavailable{ signal_test_suite_thread }

        # this allows controlling how many workers are available and unavailable
        # the worker will be unavailable until we signal it
        def work!(work_item)
          mutex, cond_var = work_item
          mutex.synchronize{ cond_var.wait(mutex) }
        end
      end
      @work_mutex    = Mutex.new
      @work_cond_var = ConditionVariable.new
      @work_item     = [@work_mutex, @work_cond_var]

      @worker_pool = DatWorkerPool.new(@worker_class, {
        :num_workers   => @num_workers,
        :logger        => TEST_LOGGER,
        :worker_params => @worker_params
      })
      @worker_pool.start
    end
    teardown do
      # ensure we wakeup any workers still stuck in their `work!`
      @work_mutex.synchronize{ @work_cond_var.broadcast }
      @worker_pool.shutdown(0)
    end

    should "know if and how many workers are available" do
      wait_for_workers_to_become_available
      assert_equal @num_workers, subject.available_worker_count
      assert_true subject.worker_available?

      # make one worker unavailable
      subject.add_work(@work_item)

      wait_for_a_worker_to_become_unavailable
      assert_equal @num_workers - 1, subject.available_worker_count
      assert_true subject.worker_available?

      # make the rest of the workers unavailable
      (@num_workers - 1).times{ subject.add_work(@work_item) }

      wait_for_workers_to_become_unavailable
      assert_equal 0, subject.available_worker_count
      assert_false subject.worker_available?

      # make one worker available
      @work_mutex.synchronize{ @work_cond_var.signal }

      wait_for_a_worker_to_become_available
      assert_equal 1, subject.available_worker_count
      assert_true subject.worker_available?

      # make the rest of the workers available
      @work_mutex.synchronize{ @work_cond_var.broadcast }

      wait_for_workers_to_become_available
      assert_equal @num_workers, subject.available_worker_count
      assert_true subject.worker_available?
    end

  end

  class WorkerCallbackTests < SystemTests
    setup do
      @worker_class = Class.new do
        include SystemTestWorker

        on_start{    params[:callbacks_called][:on_start]    = true }
        on_shutdown{ params[:callbacks_called][:on_shutdown] = true }

        on_available{   params[:callbacks_called][:on_available]   = true }
        on_unavailable{ params[:callbacks_called][:on_unavailable] = true }

        on_error{ |e, wi| params[:callbacks_called][:on_error] = true }

        before_work{ |wi| params[:callbacks_called][:before_work] = true }
        after_work{ |wi|  params[:callbacks_called][:after_work]  = true }

        on_available{   signal_test_suite_thread }
        on_unavailable{ signal_test_suite_thread }

        def work!(work_item)
          params[:finished].push(work_item)
          signal_test_suite_thread
          raise if work_item == 'error'
        end
      end
      # use one worker to simplify; we only need to see that one worker runs its
      # callbacks
      @num_workers      = 1
      @callbacks_called = {}
      @finished         = LockedArray.new

      @worker_pool = DatWorkerPool.new(@worker_class, {
        :num_workers   => @num_workers,
        :logger        => TEST_LOGGER,
        :worker_params => @worker_params.merge({
          :callbacks_called => @callbacks_called,
          :finished         => @finished
        })
      })
    end
    teardown do
      @worker_pool.shutdown(0)
    end

    should "run worker callbacks when started" do
      assert_nil @callbacks_called[:on_start]
      assert_nil @callbacks_called[:on_available]

      subject.start
      wait_for_workers_to_become_available

      assert_true @callbacks_called[:on_start]
      assert_true @callbacks_called[:on_available]
    end

    should "run worker callbacks when work is pushed" do
      subject.start
      wait_for_workers_to_become_available
      @callbacks_called.delete(:on_available)

      assert_nil @callbacks_called[:on_unavailable]
      assert_nil @callbacks_called[:before_work]
      assert_nil @callbacks_called[:after_work]

      subject.add_work(Factory.string)
      wait_for_workers do
        @finished.size == @num_workers &&
        subject.available_worker_count == @num_workers
      end

      assert_true @callbacks_called[:on_unavailable]
      assert_true @callbacks_called[:before_work]
      assert_true @callbacks_called[:after_work]
      assert_true @callbacks_called[:on_available]
    end

    should "run worker callbacks when it errors" do
      subject.start
      wait_for_workers_to_become_available
      @callbacks_called.delete(:on_available)

      assert_nil @callbacks_called[:on_unavailable]
      assert_nil @callbacks_called[:before_work]
      assert_nil @callbacks_called[:on_error]
      assert_nil @callbacks_called[:after_work]

      subject.add_work('error')
      wait_for_workers do
        @finished.size == @num_workers &&
        subject.available_worker_count == @num_workers
      end

      assert_true @callbacks_called[:on_unavailable]
      assert_true @callbacks_called[:before_work]
      assert_true @callbacks_called[:on_error]
      assert_nil @callbacks_called[:after_work]
      assert_true @callbacks_called[:on_available]
    end

    should "run callbacks when its shutdown" do
      subject.start
      wait_for_workers_to_become_available

      assert_nil @callbacks_called[:on_unavailable]
      assert_nil @callbacks_called[:on_shutdown]

      subject.shutdown(0)

      assert_true @callbacks_called[:on_unavailable]
      assert_true @callbacks_called[:on_shutdown]
    end

  end

  class ShutdownSystemTests < SystemTests
    setup do
      @worker_class = Class.new do
        include SystemTestWorker
        on_available{   signal_test_suite_thread }
        on_unavailable{ signal_test_suite_thread }

        on_error do |error, wi|
          params[:errored].push([error, wi])
        end

        # this allows controlling how long a worker takes to finish processing
        # the work item
        def work!(work_item)
          params[:work_mutex].synchronize do
            params[:work_cond_var].wait(params[:work_mutex])
          end
          params[:finished].push(work_item)
        end
      end
      @work_mutex    = Mutex.new
      @work_cond_var = ConditionVariable.new
      @finished      = LockedArray.new
      @errored       = LockedArray.new

      @worker_pool = DatWorkerPool.new(@worker_class, {
        :num_workers   => @num_workers,
        :logger        => TEST_LOGGER,
        :worker_params => @worker_params.merge({
          :work_mutex    => @work_mutex,
          :work_cond_var => @work_cond_var,
          :finished      => @finished,
          :errored       => @errored
        })
      })

      @worker_pool.start
      wait_for_workers_to_become_available

      # add 1 more work item than we have workers to handle it
      @work_items = (@num_workers + 1).times.map{ Factory.string }
      @work_items.each{ |wi| @worker_pool.add_work(wi) }
      wait_for_workers_to_become_unavailable
    end
    teardown do
      # ensure we wakeup any workers still stuck in their `work!`
      @work_mutex.synchronize{ @work_cond_var.broadcast }
      @worker_pool.shutdown(0)
    end

    should "allow any work that has been picked up to finish processing " \
           "when shutdown without a timeout" do
      assert_true @finished.empty?
      assert_true @errored.empty?

      # start the shutdown in a thread, this will hang it indefinitely because
      # it has no timeout and the workers will never exit on their own (because
      # they are waiting to be signaled by the cond var)
      shutdown_thread = Thread.new{ subject.shutdown }
      shutdown_thread.join(0.1)
      assert_equal 'sleep', shutdown_thread.status

      # allow the workers to finish working
      @work_mutex.synchronize{ @work_cond_var.broadcast }
      wait_for_workers{ @finished.size == @num_workers }

      # ensure we finished what we started processing; assert the size and
      # includes because we can't ensure the order that the work items are
      # finished
      assert_equal @num_workers, @finished.size
      @finished.values.each do |work_item|
        assert_includes work_item, @work_items[0, @num_workers]
      end

      # ensure it didn't pick up anymore work
      assert_equal [@work_items.last], subject.queue.work_items

      # ensure nothing errored
      assert_true @errored.empty?

      # ensure the shutdown exits
      shutdown_thread.join
      assert_false shutdown_thread.alive?
    end

    should "allow any work that has been picked up to finish processing " \
           "when forced to shutdown because it timed out" do
      assert_true @finished.empty?
      assert_true @errored.empty?

      # start the shutdown in a thread, this will hang until the timeout
      # finishes; this is required otherwise system timer will think we are
      # triggering a deadlock (it's not a deadlock because of the timeout)
      shutdown_thread = Thread.new{ subject.shutdown(0) }
      shutdown_thread.join(0.1)
      assert_equal 'sleep', shutdown_thread.status

      # wait for the workers to get forced to exit
      wait_for_workers{ @errored.size == @num_workers }

      # ensure it didn't finish what it started processing
      assert_true @finished.empty?

      # ensure it didn't pick up anymore work
      assert_equal [@work_items.last], subject.queue.work_items

      # ensure all the work it picked up was reported to its on-error callback
      assert_equal @num_workers, @errored.size
      @errored.values.each do |(exception, work_item)|
        assert_instance_of ShutdownError, exception
        assert_includes work_item, @work_items[0, @num_workers]
      end

      # ensure the shutdown exits
      shutdown_thread.join
      assert_false shutdown_thread.alive?
    end

  end

  module SystemTestWorker
    def self.included(klass)
      klass.class_eval{ include DatWorkerPool::Worker }
    end

    def signal_test_suite_thread
      params[:mutex].synchronize{ params[:cond_var].signal }
    end
  end

end
