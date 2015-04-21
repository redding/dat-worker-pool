require 'logger'
require 'system_timer'
require 'thread'

require 'dat-worker-pool/version'
require 'dat-worker-pool/queue'
require 'dat-worker-pool/worker'

class DatWorkerPool

  attr_reader :logger, :spawned
  attr_reader :queue
  attr_reader :on_worker_error_callbacks
  attr_reader :on_worker_start_callbacks, :on_worker_shutdown_callbacks
  attr_reader :on_worker_sleep_callbacks, :on_worker_wakeup_callbacks
  attr_reader :before_work_callbacks, :after_work_callbacks

  def initialize(min = 0, max = 1, debug = false, &do_work_proc)
    @min_workers  = min
    @max_workers  = max
    @debug        = debug
    @logger       = Logger.new(@debug)
    @do_work_proc = do_work_proc

    @queue           = Queue.new
    @workers_waiting = WorkersWaiting.new

    @mutex   = Mutex.new
    @workers = []
    @spawned = 0

    @on_worker_error_callbacks    = []
    @on_worker_start_callbacks    = []
    @on_worker_shutdown_callbacks = [proc{ |worker| despawn_worker(worker) }]
    @on_worker_sleep_callbacks    = [proc{ @workers_waiting.increment }]
    @on_worker_wakeup_callbacks   = [proc{ @workers_waiting.decrement }]
    @before_work_callbacks        = []
    @after_work_callbacks         = []

    @started = false
  end

  def start
    @started = true
    @queue.start
    @min_workers.times{ spawn_worker }
  end

  # * All work on the queue is left on the queue. It's up to the controlling
  #   system to decide how it should handle this.
  def shutdown(timeout = nil)
    @started = false
    begin
      OptionalTimeout.new(timeout){ graceful_shutdown }
    rescue TimeoutError
      force_shutdown(caller)
    end
  end

  # * Always check if all workers are busy before pushing the work because
  #   `@queue.push` can wakeup a worker. If you check after, you can see all
  #   workers are busy because one just wokeup to handle what was just pushed.
  #   This would cause it to spawn a worker when one isn't needed.
  def add_work(work_item)
    return if work_item.nil?
    new_worker_needed = self.all_spawned_workers_are_busy?
    @queue.push work_item
    spawn_worker if @started && new_worker_needed && !reached_max_workers?
  end

  def work_items
    @queue.work_items
  end

  def queue_empty?
    @queue.empty?
  end

  def waiting
    @workers_waiting.count
  end

  def worker_available?
    !reached_max_workers? || @workers_waiting.count > 0
  end

  def all_spawned_workers_are_busy?
    @workers_waiting.count <= 0
  end

  def reached_max_workers?
    @mutex.synchronize{ @spawned >= @max_workers }
  end

  def on_queue_pop_callbacks;  @queue.on_pop_callbacks;  end
  def on_queue_push_callbacks; @queue.on_push_callbacks; end

  def on_queue_pop(&block);  @queue.on_pop_callbacks  << block; end
  def on_queue_push(&block); @queue.on_push_callbacks << block; end

  def on_worker_error(&block);    @on_worker_error_callbacks    << block; end
  def on_worker_start(&block);    @on_worker_start_callbacks    << block; end
  def on_worker_shutdown(&block); @on_worker_shutdown_callbacks << block; end
  def on_worker_sleep(&block);    @on_worker_sleep_callbacks    << block; end
  def on_worker_wakeup(&block);   @on_worker_wakeup_callbacks   << block; end

  def before_work(&block); @before_work_callbacks << block; end
  def after_work(&block);  @after_work_callbacks  << block; end

  private

  def do_work(work_item)
    @do_work_proc.call(work_item)
  end

  # * Always shutdown workers before the queue. `@queue.shutdown` wakes up the
  #   workers. If you haven't told them to shutdown before they wakeup then they
  #   won't start their shutdown when they are woken up.
  # * Use `@workers.first.join until @workers.empty?` instead of `each` to join
  #   all the workers. While we are joining a worker a separate worker can
  #   shutdown and remove itself from the `@workers` array.
  def graceful_shutdown
    @workers.each(&:shutdown)
    @queue.shutdown
    @workers.first.join until @workers.empty?
  end

  # * Use `@workers.first until @workers.empty?` pattern instead of `each` to
  #   raise and join all the workers. While we are raising and joining a worker
  #   a separate worker can shutdown and remove itself from the `@workers`
  #   array.
  def force_shutdown(backtrace)
    error = ShutdownError.new("Timed out shutting down the worker pool")
    error.set_backtrace(backtrace)
    until @workers.empty?
      worker = @workers.first
      worker.raise(error)
      worker.join
    end
    raise error if @debug
  end

  def spawn_worker
    @mutex.synchronize{ spawn_worker! }
  end

  def spawn_worker!
    Worker.new(@queue).tap do |w|
      w.on_work = proc{ |worker, work_item| do_work(work_item) }

      w.on_error_callbacks    = @on_worker_error_callbacks
      w.on_start_callbacks    = @on_worker_start_callbacks
      w.on_shutdown_callbacks = @on_worker_shutdown_callbacks
      w.on_sleep_callbacks    = @on_worker_sleep_callbacks
      w.on_wakeup_callbacks   = @on_worker_wakeup_callbacks
      w.before_work_callbacks = @before_work_callbacks
      w.after_work_callbacks  = @after_work_callbacks

      @workers << w
      @spawned += 1

      w.start
    end
  end

  def despawn_worker(worker)
    @mutex.synchronize{ despawn_worker!(worker) }
  end

  def despawn_worker!(worker)
    @spawned -= 1
    @workers.delete worker
  end

  class WorkersWaiting
    attr_reader :count

    def initialize
      @mutex = Mutex.new
      @count = 0
    end

    def increment
      @mutex.synchronize{ @count += 1 }
    end

    def decrement
      @mutex.synchronize{ @count -= 1 }
    end
  end

  module OptionalTimeout
    def self.new(seconds, &block)
      if seconds
        SystemTimer.timeout(seconds, TimeoutError, &block)
      else
        block.call
      end
    end
  end

  module Logger
    def self.new(debug)
      debug ? ::Logger.new(STDOUT) : ::Logger.new(File.open('/dev/null', 'w'))
    end
  end

  TimeoutError = Class.new(RuntimeError)

  # * This error should never be "swallowed". If it is caught be sure to
  #   re-raise it so the workers shutdown. Otherwise workers will get killed
  #   (`Thread#kill`) by ruby which causes lots of issues.
  ShutdownError = Class.new(Interrupt)

end
