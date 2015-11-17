require 'assert'
require 'dat-worker-pool/locked_object'

require 'test/support/thread_spies'

class DatWorkerPool::LockedObject

  class UnitTests < Assert::Context
    setup do
      @mutex_spy = MutexSpy.new
      Assert.stub(Mutex, :new){ @mutex_spy }
    end

  end

  class LockedObjectTests < UnitTests
    desc "DatWorkerPool::LockedObject"
    setup do
      @passed_value = Factory.string
      @locked_object = DatWorkerPool::LockedObject.new(@passed_value)
    end
    subject{ @locked_object }

    should have_readers :mutex
    should have_imeths :value, :set
    should have_imeths :with_lock

    should "know its value" do
      assert_equal @passed_value, subject.value
    end

    should "default its value to `nil`" do
      assert_nil DatWorkerPool::LockedObject.new.value
    end

    should "lock access to its value" do
      assert_false @mutex_spy.synchronize_called
      subject.value
      assert_true @mutex_spy.synchronize_called
    end

    should "allow setting its value" do
      new_value = Factory.string
      subject.set(new_value)
      assert_equal new_value, subject.value
    end

    should "lock access to its value when setting it" do
      assert_false @mutex_spy.synchronize_called
      subject.set(Factory.string)
      assert_true @mutex_spy.synchronize_called
    end

    should "allow accessing its mutex and value with the lock using `with_lock`" do
      yielded_mutex, yielded_value = [nil, nil]
      assert_false @mutex_spy.synchronize_called
      subject.with_lock{ |m, v| yielded_mutex, yielded_value = [m, v] }
      assert_true @mutex_spy.synchronize_called

      assert_equal subject.mutex, yielded_mutex
      assert_equal subject.value, yielded_value
    end

  end

  class LockedArrayTests < UnitTests
    desc "DatWorkerPool::LockedArray"
    setup do
      @locked_array = DatWorkerPool::LockedArray.new
    end
    subject{ @locked_array }

    should have_imeths :values
    should have_imeths :empty?
    should have_imeths :push, :pop
    should have_imeths :shift, :unshift
    should have_imeths :delete

    should "be a dat-worker-pool locked object" do
      assert DatWorkerPool::LockedArray < DatWorkerPool::LockedObject
    end

    should "use an empty array for its value" do
      assert_equal [], subject.value
    end

    should "alias its value method as values" do
      assert_same subject.value, subject.values
    end

    should "know if its empty or not" do
      assert_true subject.empty?
      subject.value.push(Factory.string)
      assert_false subject.empty?
    end

    should "lock access to checking if its empty" do
      assert_false @mutex_spy.synchronize_called
      subject.empty?
      assert_true @mutex_spy.synchronize_called
    end

    should "allow pushing and popping to its array" do
      new_item = Factory.string
      subject.push(new_item)
      assert_same new_item, subject.value.last

      result = subject.pop
      assert_same new_item, result
    end

    should "lock accessing when pushing/popping" do
      assert_false @mutex_spy.synchronize_called
      subject.push(Factory.string)
      assert_true @mutex_spy.synchronize_called

      @mutex_spy.synchronize_called = false
      subject.pop
      assert_true @mutex_spy.synchronize_called
    end

    should "allow shifting and unshifting to its array" do
      new_item = Factory.string
      subject.unshift(new_item)
      assert_same new_item, subject.value.first

      result = subject.shift
      assert_same new_item, result
    end

    should "lock accessing when shifting/unshifting" do
      assert_false @mutex_spy.synchronize_called
      subject.unshift(Factory.string)
      assert_true @mutex_spy.synchronize_called

      @mutex_spy.synchronize_called = false
      subject.shift
      assert_true @mutex_spy.synchronize_called
    end

    should "allow deleting items from its array" do
      item = Factory.string
      (Factory.integer(3) + 1).times{ subject.push(item) }

      subject.delete(item)
      assert_true subject.empty?
    end

    should "lock access to when deleting" do
      assert_false @mutex_spy.synchronize_called
      subject.delete(Factory.string)
      assert_true @mutex_spy.synchronize_called
    end

  end

  class LockedSetTests < UnitTests
    desc "DatWorkerPool::LockedSet"
    setup do
      @locked_set = DatWorkerPool::LockedSet.new
    end
    subject{ @locked_set }

    should have_imeths :values, :size, :empty?, :add, :remove

    should "be a dat-worker-pool locked object" do
      assert DatWorkerPool::LockedSet < DatWorkerPool::LockedObject
    end

    should "use an empty set for its value" do
      assert_instance_of Set, subject.value
      assert_true subject.value.empty?
    end

    should "alias its value method as values" do
      assert_same subject.value, subject.values
    end

    should "know its size" do
      assert_equal 0, subject.size
      subject.value.add(Factory.string)
      assert_equal 1, subject.size
    end

    should "lock access to reading its size" do
      assert_false @mutex_spy.synchronize_called
      subject.size
      assert_true @mutex_spy.synchronize_called
    end

    should "know if its empty or not" do
      assert_true subject.empty?
      subject.value.add(Factory.string)
      assert_false subject.empty?
    end

    should "lock access to checking if its empty" do
      assert_false @mutex_spy.synchronize_called
      subject.empty?
      assert_true @mutex_spy.synchronize_called
    end

    should "allow adding and removing on its set" do
      new_item = Factory.string
      subject.add(new_item)
      assert_includes new_item, subject.value

      subject.remove(new_item)
      assert_not_includes new_item, subject.value
    end

    should "lock accessing when pushing/popping" do
      assert_false @mutex_spy.synchronize_called
      subject.add(Factory.string)
      assert_true @mutex_spy.synchronize_called

      @mutex_spy.synchronize_called = false
      subject.remove(Factory.string)
      assert_true @mutex_spy.synchronize_called
    end

  end

end
