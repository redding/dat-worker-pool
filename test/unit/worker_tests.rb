require 'assert'
require 'dat-worker-pool/worker'

require 'dat-worker-pool'
require 'dat-worker-pool/queue'

class DatWorkerPool::Worker

  class UnitTests < Assert::Context
    desc "DatWorkerPool::Worker"
    setup do
      @queue = DatWorkerPool::Queue.new
      @work_done = []
      @worker = DatWorkerPool::Worker.new(@queue).tap do |w|
        w.on_work = proc{ |work| @work_done << work }
      end
    end
    teardown do
      @worker.shutdown
      @worker.join
    end
    subject{ @worker }

    should have_writers :on_waiting, :on_continuing, :on_shutdown
    should have_imeths :start, :shutdown, :join, :running?

    should "start a thread with it's work loop with #start" do
      thread = nil
      assert_nothing_raised{ thread = subject.start }
      assert_instance_of Thread, thread
      assert thread.alive?
      assert subject.running?
    end

    should "call the block it's passed when it get's work from the queue" do
      subject.start
      @queue.push 'one'
      subject.join 0.1 # trigger the worker's thread to run
      @queue.push 'two'
      subject.join 0.1 # trigger the worker's thread to run
      assert_equal [ 'one', 'two' ], @work_done
    end

    should "stop it's queue and itself, ending it's thread with #shutdown" do
      thread = subject.start
      subject.join 0.1 # trigger the worker's thread to run, allow it to get into it's
                       # work loop
      assert_nothing_raised{ subject.shutdown }
      subject.join 0.1 # trigger the worker's thread to run, should exit
      assert_not thread.alive?
      assert_not subject.running?
    end

  end

  class CallbacksTests < UnitTests
    desc "callbacks"
    setup do
      @worker = DatWorkerPool::Worker.new(@queue).tap do |w|
        w.on_work = proc{ |work| sleep 0.2 }
      end
    end

    should "call the on waiting callback, yielding itself, when " \
           "it's waiting on work from the queue" do
      waiting, yielded_worker = nil, nil
      subject.on_waiting = proc do |worker|
        waiting = true
        yielded_worker = worker
      end
      subject.start
      subject.join 0.1 # trigger the worker's thread to run

      assert_equal true, waiting
      assert_equal subject, yielded_worker
    end

    should "call the on continuing callback, yielding itself, when " \
           "it's done waiting for work from the queue" do
      waiting, yielded_worker = nil, nil
      subject.on_continuing = proc do |worker|
        waiting = false
        yielded_worker = worker
      end
      subject.start
      @queue.push 'some work'
      subject.join 0.1 # trigger the worker's thread to run

      assert_equal false, waiting
      assert_equal subject, yielded_worker
    end

    should "call the on shutdown callback, yielding itself, when " \
           "it's shutdown and exits it's work loop" do
      shutdown, yielded_worker = nil, nil
      subject.on_shutdown = proc do |worker|
        shutdown = true
        yielded_worker = worker
      end
      subject.start
      subject.shutdown
      subject.join 0.1 # trigger the worker's thread to run

      assert_equal true, shutdown
      assert_equal subject, yielded_worker
    end

  end

end
