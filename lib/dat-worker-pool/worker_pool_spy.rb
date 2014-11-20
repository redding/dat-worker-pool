class DatWorkerPool

  class WorkerPoolSpy
    attr_reader :work_items
    attr_reader :start_called
    attr_reader :shutdown_called, :shutdown_timeout
    attr_accessor :worker_available

    def initialize
      @worker_available = false
      @work_items = []
      @start_called = false
      @shutdown_called  = false
      @shutdown_timeout = nil
    end

    def worker_available?
      @worker_available
    end

    def queue_empty?
      @work_items.empty?
    end

    def add_work(work)
      @work_items << work if work
    end

    def start
      @start_called = true
    end

    def shutdown(timeout = nil)
      @shutdown_called = true
      @shutdown_timeout = timeout
    end
  end

end
