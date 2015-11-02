class DatWorkerPool

  class WorkerPoolSpy

    attr_reader :num_workers, :debug
    attr_reader :work_proc, :work_items
    attr_reader :start_called
    attr_reader :shutdown_called, :shutdown_timeout
    attr_reader :on_queue_pop_callbacks, :on_queue_push_callbacks
    attr_reader :on_worker_error_callbacks
    attr_reader :on_worker_start_callbacks, :on_worker_shutdown_callbacks
    attr_reader :on_worker_sleep_callbacks, :on_worker_wakeup_callbacks
    attr_reader :before_work_callbacks, :after_work_callbacks
    attr_accessor :worker_available

    def initialize(num_workers = 1, debug = false, &block)
      @num_workers = num_workers
      @debug       = debug
      @work_proc   = block

      @worker_available = false
      @work_items = []
      @start_called = false
      @shutdown_called  = false
      @shutdown_timeout = nil

      @on_queue_pop_callbacks       = []
      @on_queue_push_callbacks      = []
      @on_worker_error_callbacks    = []
      @on_worker_start_callbacks    = []
      @on_worker_shutdown_callbacks = []
      @on_worker_sleep_callbacks    = []
      @on_worker_wakeup_callbacks   = []
      @before_work_callbacks        = []
      @after_work_callbacks         = []
    end

    def start
      @start_called = true
    end

    def shutdown(timeout = nil)
      @shutdown_called = true
      @shutdown_timeout = timeout
    end

    def add_work(work)
      return unless work
      @work_items << work
      @on_queue_push_callbacks.each(&:call)
    end

    def pop_work
      work = @work_items.shift
      @on_queue_pop_callbacks.each(&:call)
      work
    end

    def queue_empty?
      @work_items.empty?
    end

    def worker_available?
      @worker_available
    end

    def on_queue_pop(&block);       @on_queue_pop_callbacks       << block; end
    def on_queue_push(&block);      @on_queue_push_callbacks      << block; end
    def on_worker_error(&block);    @on_worker_error_callbacks    << block; end
    def on_worker_start(&block);    @on_worker_start_callbacks    << block; end
    def on_worker_shutdown(&block); @on_worker_shutdown_callbacks << block; end
    def on_worker_sleep(&block);    @on_worker_sleep_callbacks    << block; end
    def on_worker_wakeup(&block);   @on_worker_wakeup_callbacks   << block; end
    def before_work(&block);        @before_work_callbacks        << block; end
    def after_work(&block);         @after_work_callbacks         << block; end

  end

end
