require 'assert'
require 'dat-worker-pool'

class DatWorkerPool

  class SystemTests < Assert::Context
    desc "DatWorkerPool"

  end

  class UseWorkerPoolTests < SystemTests
    setup do
      @mutex = Mutex.new
      @results = []
      @work_pool = DatWorkerPool.new(2, !!ENV['DEBUG']) do |work|
        @mutex.synchronize{ @results << (work * 100) }
      end
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

  class ForcedShutdownSystemTests < SystemTests
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
    subject{ @work_pool }

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
