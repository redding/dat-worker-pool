require 'assert'
require 'dat-worker-pool'

class UseWorkerPoolTests < Assert::Context

  desc "defining a custom worker pool"
  setup do
    @mutex = Mutex.new
    @results = []
    @work_pool = DatWorkerPool.new(1, 2, !!ENV['DEBUG']) do |work|
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
