require 'assert'
require 'dat-worker-pool'

class DatWorkerPool

  class BaseTests < Assert::Context
    desc "DatWorkerPool"
    setup do
      @work_pool = DatWorkerPool.new{ }
    end
    subject{ @work_pool }

    should have_readers :logger, :spawned
    should have_imeths :add_work, :shutdown, :despawn_worker
    should have_imeths :work_items, :waiting

  end

  class WorkerBehaviorTests < BaseTests
    desc "workers"
    setup do
      @work_pool = DatWorkerPool.new(1, 2, true){|work| sleep(work) }
    end

    should "be created as needed and only go up to the maximum number allowed" do
      # the minimum should be spawned and waiting
      assert_equal 1, @work_pool.spawned
      assert_equal 1, @work_pool.waiting

      # the minimum should be spawned, but no longer waiting
      @work_pool.add_work 5
      assert_equal 1, @work_pool.spawned
      assert_equal 0, @work_pool.waiting

      # an additional worker should be spawned
      @work_pool.add_work 5
      assert_equal 2, @work_pool.spawned
      assert_equal 0, @work_pool.waiting

      # no additional workers are spawned, the work waits to be processed
      @work_pool.add_work 5
      assert_equal 2, @work_pool.spawned
      assert_equal 0, @work_pool.waiting
    end

    should "go back to waiting when they finish working" do
      assert_equal 1, @work_pool.spawned
      assert_equal 1, @work_pool.waiting

      @work_pool.add_work 1
      assert_equal 1, @work_pool.spawned
      assert_equal 0, @work_pool.waiting

      sleep 1 # allow the worker to run

      assert_equal 1, @work_pool.spawned
      assert_equal 1, @work_pool.waiting
    end

  end

  class AddWorkAndProcessItTests < BaseTests
    desc "add_work and process"
    setup do
      @result = nil
      @work_pool = DatWorkerPool.new(1){|work| @result = (2 / work) }
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

      assert_equal 1, subject.spawned
      assert_equal 1, subject.waiting
      assert worker.instance_variable_get("@thread").alive?
    end

  end

  class ShutdownTests < BaseTests
    desc "shutdown"
    setup do
      @mutex = Mutex.new
      @finished = []
      @work_pool = DatWorkerPool.new(1, 2, true) do |work|
        sleep 1
        @mutex.synchronize{ @finished << work }
      end
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

      assert_equal 0, subject.spawned
      assert_equal 0, subject.waiting
      assert_includes 'c', subject.work_items
    end

    should "timeout if the workers take to long to finish" do
      # make sure the workers haven't processed any work
      assert_equal [], @finished
      assert_raises(DatWorkerPool::TimeoutError) do
        subject.shutdown(0.1)
      end
    end

  end

end
