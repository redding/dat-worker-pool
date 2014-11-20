require 'assert'
require 'dat-worker-pool/queue'

class DatWorkerPool::Queue

  class UnitTests < Assert::Context
    desc "DatWorkerPool::Queue"
    setup do
      @queue = DatWorkerPool::Queue.new
    end
    subject{ @queue }

    should have_imeths :work_items, :push, :pop, :empty?
    should have_imeths :start, :shutdown, :shutdown?

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

    should "return nothing with pop when the queue has been shutdown" do
      subject.push 'work1'
      subject.shutdown
      assert_nil subject.pop
    end

    should "return whether the queue is empty or not with #empty?" do
      assert subject.empty?
      subject.push 'work'
      assert_not subject.empty?
      subject.pop
      assert subject.empty?
    end

    should "reset its shutdown flag when started" do
      assert_false subject.shutdown?
      subject.shutdown
      assert_true subject.shutdown?
      subject.start
      assert_false subject.shutdown?
    end

  end

  class SignallingTests < UnitTests
    desc "mutex and condition variable behavior"
    setup do
      @thread = Thread.new do
        Thread.current['work_item'] = @queue.pop || 'got nothing'
      end
    end

    should "have threads wait for a work item to be added when using pop" do
      assert_equal "sleep", @thread.status
    end

    should "wakeup threads when work is pushed onto the queue" do
      subject.push 'some work'
      sleep 0.1
      assert !@thread.alive?
      assert_equal 'some work', @thread['work_item']
    end

    should "wakeup thread when the queue is shutdown" do
      subject.shutdown
      sleep 0.1
      assert !@thread.alive?
      assert_equal 'got nothing', @thread['work_item']
    end

  end

end
