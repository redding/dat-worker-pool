require 'assert'
require 'dat-worker-pool/worker'

require 'dat-worker-pool'
require 'dat-worker-pool/queue'

class DatWorkerPool::Worker

  class BaseTests < Assert::Context
    desc "DatWorkerPool::Worker"
    setup do
      @pool  = DatWorkerPool.new{ }
      @queue = DatWorkerPool::Queue.new
      @workers_waiting = DatWorkerPool::WorkersWaiting.new
      @worker = DatWorkerPool::Worker.new(@pool, @queue, @workers_waiting){ }
    end
    subject{ @worker }

    should have_imeths :shutdown, :join

    should "trigger exiting it's work loop with #shutdown and " \
           "join it's thread with #join" do
      @queue.shutdown   # ensure the thread is not waiting on the queue
      subject.join(0.1) # ensure the thread is looping for work
      subject.shutdown
      assert_not_nil subject.join(1)
    end

  end

end
