require 'assert'
require 'dat-worker-pool/worker'

module DatWorkerPool::Worker

  class UnitTests < Assert::Context
    desc "DatWorkerPool::Worker"
    setup do
      @worker_class = Class.new do
        include DatWorkerPool::Worker
      end
    end
    subject{ @worker_class }

    should have_imeths :on_start_callbacks, :on_shutdown_callbacks
    should have_imeths :on_sleep_callbacks, :on_wakeup_callbacks
    should have_imeths :on_error_callbacks
    should have_imeths :before_work_callbacks, :after_work_callbacks
    should have_imeths :on_start, :on_shutdown
    should have_imeths :on_sleep, :on_wakeup
    should have_imeths :on_error
    should have_imeths :before_work, :after_work
    should have_imeths :prepend_on_start, :prepend_on_shutdown
    should have_imeths :prepend_on_sleep, :prepend_on_wakeup
    should have_imeths :prepend_on_error
    should have_imeths :prepend_before_work, :prepend_after_work

    should "not have any callbacks by default" do
      assert_equal [], subject.on_start_callbacks
      assert_equal [], subject.on_shutdown_callbacks
      assert_equal [], subject.on_sleep_callbacks
      assert_equal [], subject.on_wakeup_callbacks
      assert_equal [], subject.on_error_callbacks
      assert_equal [], subject.before_work_callbacks
      assert_equal [], subject.after_work_callbacks
    end

    should "allow appending callbacks" do
      callback = proc{ Factory.string }
      # add a callback to each type to show we are appending
      subject.on_start_callbacks    << proc{ Factory.string }
      subject.on_shutdown_callbacks << proc{ Factory.string }
      subject.on_sleep_callbacks    << proc{ Factory.string }
      subject.on_wakeup_callbacks   << proc{ Factory.string }
      subject.on_error_callbacks    << proc{ Factory.string }
      subject.before_work_callbacks << proc{ Factory.string }
      subject.after_work_callbacks  << proc{ Factory.string }

      subject.on_start(&callback)
      assert_equal callback, subject.on_start_callbacks.last

      subject.on_shutdown(&callback)
      assert_equal callback, subject.on_shutdown_callbacks.last

      subject.on_sleep(&callback)
      assert_equal callback, subject.on_sleep_callbacks.last

      subject.on_wakeup(&callback)
      assert_equal callback, subject.on_wakeup_callbacks.last

      subject.on_error(&callback)
      assert_equal callback, subject.on_error_callbacks.last

      subject.before_work(&callback)
      assert_equal callback, subject.before_work_callbacks.last

      subject.after_work(&callback)
      assert_equal callback, subject.after_work_callbacks.last
    end

    should "allow prepending callbacks" do
      callback = proc{ Factory.string }
      # add a callback to each type to show we are appending
      subject.on_start_callbacks    << proc{ Factory.string }
      subject.on_shutdown_callbacks << proc{ Factory.string }
      subject.on_sleep_callbacks    << proc{ Factory.string }
      subject.on_wakeup_callbacks   << proc{ Factory.string }
      subject.on_error_callbacks    << proc{ Factory.string }
      subject.before_work_callbacks << proc{ Factory.string }
      subject.after_work_callbacks  << proc{ Factory.string }

      subject.prepend_on_start(&callback)
      assert_equal callback, subject.on_start_callbacks.first

      subject.prepend_on_shutdown(&callback)
      assert_equal callback, subject.on_shutdown_callbacks.first

      subject.prepend_on_sleep(&callback)
      assert_equal callback, subject.on_sleep_callbacks.first

      subject.prepend_on_wakeup(&callback)
      assert_equal callback, subject.on_wakeup_callbacks.first

      subject.prepend_on_error(&callback)
      assert_equal callback, subject.on_error_callbacks.first

      subject.prepend_before_work(&callback)
      assert_equal callback, subject.before_work_callbacks.first

      subject.prepend_after_work(&callback)
      assert_equal callback, subject.after_work_callbacks.first
    end

  end

  class InitTests < UnitTests

  end

end
