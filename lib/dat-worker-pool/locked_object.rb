require 'thread'

class DatWorkerPool

  class LockedObject
    attr_reader :mutex

    def initialize(object = nil)
      @object = object
      @mutex  = Mutex.new
    end

    def value
      @mutex.synchronize{ @object }
    end

    def set(new_object)
      @mutex.synchronize{ @object = new_object }
    end

    def with_lock(&block)
      @mutex.synchronize{ block.call(@mutex, @object) }
    end

  end

  class LockedArray < LockedObject
    def initialize(array = nil)
      super(array || [])
    end

    alias :values :value

    def first;  @mutex.synchronize{ @object.first };  end
    def last;   @mutex.synchronize{ @object.last };   end
    def size;   @mutex.synchronize{ @object.size };   end
    def empty?; @mutex.synchronize{ @object.empty? }; end

    def push(new_item); @mutex.synchronize{ @object.push(new_item) }; end
    def pop;            @mutex.synchronize{ @object.pop };            end

    def shift;             @mutex.synchronize{ @object.shift };             end
    def unshift(new_item); @mutex.synchronize{ @object.unshift(new_item) }; end

    def delete(item); @mutex.synchronize{ @object.delete(item) }; end
  end

  class LockedSet < LockedObject
    def initialize; super(Set.new); end

    alias :values :value

    def size;   @mutex.synchronize{ @object.size };   end
    def empty?; @mutex.synchronize{ @object.empty? }; end

    def add(item);    @mutex.synchronize{ @object.add(item) };    end
    def remove(item); @mutex.synchronize{ @object.delete(item) }; end
  end

end
