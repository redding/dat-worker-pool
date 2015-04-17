require 'logger'
require 'system_timer'
require 'thread'

require 'dat-worker-pool/version'
require 'dat-worker-pool/queue'
require 'dat-worker-pool/worker'

class DatWorkerPool

  attr_reader :logger, :spawned
  attr_reader :queue
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

    @on_worker_start_callbacks    = []
    @on_worker_shutdown_callbacks = [proc{ |worker| despawn_worker(worker) }]
    @on_worker_sleep_callbacks    = [proc{ @workers_waiting.increment }]
    @on_worker_wakeup_callbacks   = [proc{ @workers_waiting.decrement }]
    @before_work_callbacks = []
    @after_work_callbacks  = []

    @started = false
  end

  def work_items
    @queue.work_items
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

  def queue_empty?
    @queue.empty?
  end

  # Check if all workers are busy before adding the work. When the work is
  # added, a worker will stop waiting (if it was idle). Because of that, we
  # can't reliably check if all workers are busy. We might think all workers are
  # busy because we just woke up a sleeping worker to process this work. Then we
  # would spawn a worker to do nothing.
  def add_work(work_item)
    return if work_item.nil?
    new_worker_needed = all_spawned_workers_are_busy?
    @queue.push work_item
    self.spawn_worker if @started && new_worker_needed && !reached_max_workers?
  end

  def start
    @started = true
    @queue.start
    @min_workers.times{ self.spawn_worker }
  end

  # Shutdown each worker and then the queue. Shutting down the queue will
  # signal any workers waiting on it to wake up, so they can start shutting
  # down. If a worker is processing work, then it will be joined and allowed to
  # finish.
  # **NOTE** Any work that is left on the queue isn't processed. The controlling
  # application for the worker pool should gracefully handle these items.
  # **NOTE** Use the `@workers.first until @workers.empty?` pattern instead of
  # `each`. We don't want to call `join` or `raise` on every worker (especially
  # if they are shutting down on their own). This is safe, otherwise you might
  # get a dead thread in the `each`.
  def shutdown(timeout = nil)
    @started = false
    begin
      proc = OptionalTimeoutProc.new(timeout, true) do
        # Workers need to be shutdown before the queue. This marks a flag that
        # tells the workers to exit out of their loop once they wakeup. The
        # queue shutdown signals the workers to wakeup, so the flag needs to be
        # set before they wakeup.
        @workers.each(&:shutdown)
        @queue.shutdown
        @workers.first.join until @workers.empty?
      end
      proc.call
    rescue ShutdownError => exception
      exception.message.replace "Timed out shutting down the worker pool"
      exception.set_backtrace(caller)
      @workers.first.raise(exception) until @workers.empty?
      @debug ? raise(exception) : self.logger.error(exception.message)
    end
  end

  def on_queue_pop_callbacks;  @queue.on_pop_callbacks;  end
  def on_queue_push_callbacks; @queue.on_push_callbacks; end

  def on_queue_pop(&block);  @queue.on_pop_callbacks << block;  end
  def on_queue_push(&block); @queue.on_push_callbacks << block; end

  def on_worker_start(&block);    @on_worker_start_callbacks << block;    end
  def on_worker_shutdown(&block); @on_worker_shutdown_callbacks << block; end
  def on_worker_sleep(&block);    @on_worker_sleep_callbacks << block;    end
  def on_worker_wakeup(&block);   @on_worker_wakeup_callbacks << block;   end

  def before_work(&block); @before_work_callbacks << block; end
  def after_work(&block);  @after_work_callbacks << block;  end

  protected

  def spawn_worker
    @mutex.synchronize do
      Worker.new(@queue).tap do |w|
        w.on_work = proc{ |worker, work_item| do_work(work_item) }
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
  end

  def despawn_worker(worker)
    @mutex.synchronize do
      @spawned -= 1
      @workers.delete worker
    end
  end

  def do_work(work_item)
    @do_work_proc.call(work_item)
  rescue StandardError => exception
    self.logger.error "Exception raised while doing work!"
    self.logger.error "#{exception.class}: #{exception.message}"
    self.logger.error exception.backtrace.join("\n")
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

  class OptionalTimeoutProc
    def initialize(timeout, reraise = false, &proc)
      @timeout = timeout
      @reraise = reraise
      @proc    = proc
    end

    def call
      if @timeout
        begin
          SystemTimer.timeout(@timeout, ShutdownError, &@proc)
        rescue ShutdownError
          raise if @reraise
        end
      else
        @proc.call
      end
    end
  end

  module Logger
    def self.new(debug)
      debug ? ::Logger.new(STDOUT) : ::Logger.new(File.open('/dev/null', 'w'))
    end
  end

  # This error should never be "swallowed". If it is caught then be sure to
  # re-raise it so the workers shutdown. Otherwise workers will be Thread#kill
  # by ruby which causes lots of issues.
  ShutdownError = Class.new(Interrupt)

end
