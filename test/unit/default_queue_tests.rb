require 'assert'
require 'dat-worker-pool/default_queue'

require 'dat-worker-pool/queue'

class DatWorkerPool::DefaultQueue

  class UnitTests < Assert::Context
    desc "DatWorkerPool::DefaultQueue"
    setup do
      @queue_class = DatWorkerPool::DefaultQueue
    end
    subject{ @queue_class }

    should "be a dat-worker-pool queue" do
      assert_includes DatWorkerPool::Queue, subject
    end

  end

  class InitTests < UnitTests
    desc "when init"
    setup do
      @queue = @queue_class.new
    end
    subject{ @queue }

    should have_readers :on_push_callbacks, :on_pop_callbacks
    should have_imeths :work_items, :empty?
    should have_imeths :on_push, :on_pop

    should "default its callbacks" do
      assert_equal [], subject.on_push_callbacks
      assert_equal [], subject.on_pop_callbacks
    end

    should "allow adding custom push and pop callbacks" do
      callback = proc{ Factory.string }
      subject.on_push(&callback)
      assert_includes callback, subject.on_push_callbacks

      callback = proc{ Factory.string }
      subject.on_pop(&callback)
      assert_includes callback, subject.on_pop_callbacks
    end

  end

  class StartedTests < InitTests
    desc "and started"
    setup do
      @queue.start
    end

    should "allow pushing work items onto the queue with #push" do
      subject.push 'work'
      assert_equal [ 'work' ], subject.work_items
    end

    should "call its on push callback when work is pushed" do
      on_push_called = false
      subject.on_push_callbacks << proc{ on_push_called = true }
      subject.push 'work'
      assert_true on_push_called
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

    should "call its on pop callback when work is popped" do
      subject.push 'work'
      on_pop_called = false
      subject.on_pop_callbacks << proc{ on_pop_called = true }
      subject.pop
      assert_true on_pop_called
    end

    should "return whether the queue is empty or not with #empty?" do
      assert subject.empty?
      subject.push 'work'
      assert_not subject.empty?
      subject.pop
      assert subject.empty?
    end

  end

  class SignallingTests < StartedTests
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
