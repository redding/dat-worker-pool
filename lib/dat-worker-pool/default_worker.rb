require 'thread'
require 'dat-worker-pool'
require 'dat-worker-pool/worker'

class DatWorkerPool

  class DefaultWorker
    include DatWorkerPool::Worker

    attr_accessor :on_work

    def initialize(*args)
      super
      @on_work = proc{ |work_item| }
    end

    def work!(work_item)
      @on_work.call(work_item)
    end

  end

end
