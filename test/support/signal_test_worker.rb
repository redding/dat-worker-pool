require 'much-timeout'
require 'dat-worker-pool/worker'

module SignalTestWorker
  def self.included(klass)
    klass.class_eval{ include DatWorkerPool::Worker }
  end

  private

  def signal_test_suite_thread
    params[:signal_worker_mutex].synchronize do
      params[:signal_worker_cond_var].signal
    end
  end

  module TestHelpers

    def self.included(klass)
      klass.class_eval do
        setup do
          # at least 2 workers, up to 4
          @num_workers = Factory.integer(3) + 1

          @signal_worker_mutex    = Mutex.new
          @signal_worker_cond_var = ConditionVariable.new

          @worker_params = {
            :signal_worker_mutex    => @signal_worker_mutex,
            :signal_worker_cond_var => @signal_worker_cond_var
          }
        end
      end
    end

    private

    # this could loop forever so ensure it doesn't by using a timeout
    def wait_for_workers(&block)
      MuchTimeout.just_timeout(1, {
        :do => proc{
          while !block.call do
            @signal_worker_mutex.synchronize do
              @signal_worker_cond_var.wait(@signal_worker_mutex)
            end
          end
        },
        :on_timeout => proc{
          raise "timed out waiting for workers" unless block.call
        }
      })
    end

    def wait_for_workers_to_become_available
      wait_for_workers{ subject.available_worker_count == @num_workers }
    end

    def wait_for_workers_to_become_unavailable
      wait_for_workers{ subject.available_worker_count == 0 }
    end

    def wait_for_a_worker_to_become_available
      wait_for_workers{ subject.available_worker_count != 0 }
    end

    def wait_for_a_worker_to_become_unavailable
      wait_for_workers{ subject.available_worker_count != @num_workers }
    end

  end
end
