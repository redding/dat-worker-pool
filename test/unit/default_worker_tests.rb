require 'assert'
require 'dat-worker-pool/default_worker'

require 'dat-worker-pool'
require 'dat-worker-pool/default_queue'
require 'dat-worker-pool/runner'
require 'dat-worker-pool/worker'

class DatWorkerPool::DefaultWorker

  class UnitTests < Assert::Context
    desc "DatWorkerPool::DefaultWorker"
    setup do
      @worker_class = DatWorkerPool::DefaultWorker
    end
    subject{ @worker_class }

    should "be a dat worker pool worker" do
      assert_includes DatWorkerPool::Worker, subject
    end

  end

  class InitTests < UnitTests
    setup do
      @queue  = DatWorkerPool::DefaultQueue.new.tap(&:start)
      @runner = DatWorkerPool::Runner.new(:queue => @queue)

      @work_done = []

      @worker = @worker_class.new(@runner, @queue)
      @worker.on_work = proc{ |work_item| @work_done << work_item }
    end
    teardown do
      @worker.shutdown
      @queue.shutdown
      @worker.join
    end
    subject{ @worker }

    should have_accessors :on_work

    should "call the block it's passed when it gets work from the queue" do
      subject.start
      work_items = Factory.integer(3).times.map{ Factory.string }
      work_items.each{ |work_item| @queue.push(work_item) }

      subject.join 0.1 # trigger the workers thread to run
      assert_equal work_items, @work_done
    end

  end

end
