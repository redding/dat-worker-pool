require 'assert'
require 'dat-worker-pool/queue'

class DatWorkerPool::Queue

  class BaseTests < Assert::Context
    desc "DatWorkerPool::Queue"
    setup do
      @queue = DatWorkerPool::Queue.new
    end
    subject{ @queue }

    should have_imeths :work_items, :push, :pop, :empty?
    should have_imeths :wait_for_work_item, :shutdown

    should "allow pushing work items onto the queue with #push" do
      subject.push 'work'
      assert_equal [ 'work' ], subject.work_items
    end

    should "raise an exception if trying to push work when shutdown" do
      subject.shutdown
      assert_raises(RuntimeError){ subject.push('work') }
    end

    should "pop work items off the queue with #pop" do
      subject.push 'work1'
      subject.push 'work2'

      assert_equal 2, subject.work_items.size
      assert_equal 'work1', subject.pop
      assert_equal 1, subject.work_items.size
      assert_equal 'work2', subject.pop
      assert_equal 0, subject.work_items.size
    end

    should "return whether the queue is empty or not with #empty?" do
      assert subject.empty?
      subject.push 'work'
      assert_not subject.empty?
      subject.pop
      assert subject.empty?
    end

    should "sleep a thread with #wait_for_work_item and " \
           "wake it up with #push" do
      thread = Thread.new{ subject.wait_for_work_item }
      thread.join(0.1) # ensure the thread runs enough to start waiting
      subject.push 'work'
      # if this returns nil, then the thread never finished
      assert_not_nil thread.join(1)
    end

    should "sleep threads with #wait_for_work_item and " \
           "wake them all up with #shutdown" do
      thread1 = Thread.new{ subject.wait_for_work_item }
      thread1.join(0.1) # ensure the thread runs enough to start waiting
      thread2 = Thread.new{ subject.wait_for_work_item }
      thread2.join(0.1) # ensure the thread runs enough to start waiting
      subject.shutdown
      # if these returns nil, then the threads never finished
      assert_not_nil thread1.join(1)
      assert_not_nil thread2.join(1)
    end

  end

end
