require 'assert'
require 'dat-worker-pool/default_queue'

require 'thread'
require 'dat-worker-pool/queue'
require 'test/support/thread_spies'

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
      @mutex_spy = MutexSpy.new
      Assert.stub(Mutex, :new){ @mutex_spy }

      @cond_var_spy = ConditionVariableSpy.new
      Assert.stub(ConditionVariable, :new){ @cond_var_spy }

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

    should "default its work items" do
      assert_equal [], subject.work_items
    end

    should "lock access to its work items" do
      assert_false @mutex_spy.synchronize_called
      subject.work_items
      assert_true @mutex_spy.synchronize_called
    end

    should "know if its empty or not" do
      assert_true subject.empty?
      subject.start
      subject.push(Factory.string)
      assert_false subject.empty?
    end

    should "lock access to checking if its empty" do
      assert_false @mutex_spy.synchronize_called
      subject.empty?
      assert_true @mutex_spy.synchronize_called
    end

  end

  class StartedTests < InitTests
    desc "and started"
    setup do
      @queue.start
    end

    should "broadcast to all threads when shutdown" do
      assert_false @mutex_spy.synchronize_called
      assert_false @cond_var_spy.broadcast_called
      subject.shutdown
      assert_true @cond_var_spy.broadcast_called
      assert_true @mutex_spy.synchronize_called
    end

    should "be able to add work items" do
      work_item = Factory.string
      subject.push(work_item)
      assert_equal 1, subject.work_items.size
      assert_equal work_item, subject.work_items.last

      subject.push(work_item)
      assert_equal 2, subject.work_items.size
      assert_equal work_item, subject.work_items.last
    end

    should "signal threads waiting on its lock when adding work items" do
      assert_false @mutex_spy.synchronize_called
      assert_false @cond_var_spy.signal_called
      subject.push(Factory.string)
      assert_true @cond_var_spy.signal_called
      assert_true @mutex_spy.synchronize_called
    end

    should "run on push callbacks when adding work items" do
      on_push_callback_called = false
      subject.on_push{ on_push_callback_called = true }
      subject.push(Factory.string)
      assert_true on_push_callback_called
    end

    should "be able to pop work items" do
      values = (Factory.integer(3) + 1).times.map{ Factory.string }
      values.each{ |v| subject.push(v) }

      assert_equal values.first, subject.pop
      exp = values - [values.first]
      assert_equal exp, subject.work_items
    end

    should "run on pop callbacks when popping work items" do
      subject.push(Factory.string)

      on_pop_callback_called = false
      subject.on_pop{ on_pop_callback_called = true }
      subject.pop
      assert_true on_pop_callback_called
    end

  end

  class ThreadTests < StartedTests
    desc "with a thread using it"
    setup do
      @thread = Thread.new{ Thread.current['popped_value'] = @queue.pop }
    end
    subject{ @thread }

    should "sleep the thread if empty when popping work items" do
      assert_equal 'sleep', subject.status
      assert_equal @mutex_spy, @cond_var_spy.wait_called_on
    end

    should "wakeup the thread (from waiting on pop) when work items are added" do
      assert_equal 'sleep', subject.status

      value = Factory.string
      @queue.push(value)

      assert_not subject.alive?
      assert_equal value, subject['popped_value']
    end

    should "re-sleep the thread if woken up while the queue is empty" do
      assert_equal 'sleep', subject.status
      assert_equal 1, @cond_var_spy.wait_call_count

      # wakeup the thread (like `push` does) but we don't want to add anything
      # to the queue, its possible this can happen if another worker never
      # sleeps and grabs the lock and work item before the thread being woken
      # up
      @mutex_spy.synchronize{ @cond_var_spy.signal }

      assert_equal 'sleep', subject.status
      assert_equal 2, @cond_var_spy.wait_call_count
    end

    should "wakeup the thread (from waiting on pop) when its shutdown" do
      assert_equal 'sleep', subject.status
      assert_equal 1, @cond_var_spy.wait_call_count

      @queue.shutdown

      assert_not subject.alive?
      assert_equal 1, @cond_var_spy.wait_call_count
    end

    should "not pop a work item when shutdown and not empty" do
      assert_equal 'sleep', subject.status

      # this is to simulate a specific situation where there is work on the
      # queue and the queue gets shutdown while a worker is still sleeping (it
      # hasn't gotten a chance to wakeup and pull work off the queue yet), if
      # this happens we don't want it to pull work off the queue; to set up this
      # scenario we can't use `push` because it will wakeup the thread; so this
      # accesses the array directly and pushes an item on it
      @queue.instance_variable_get("@work_items") << Factory.string
      @queue.shutdown

      assert_not subject.alive?
      assert_nil subject['popped_value']
    end

  end

end
