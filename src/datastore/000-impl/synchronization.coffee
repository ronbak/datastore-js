# @private
#
# Basic usage is
#
# await sync_queue.request defer(), priority_data
#   [ run some code here, can be async with await/defers ]
# sync_queue.finish()
#
# Two code blocks using the same SyncQueue will not run in an
# interleaved manner; instead, the second block will be queued up to
# run after the first one finishes.
#
# TODO: log warning if a request takes an unusually long time (could
# be forgetting to call finish())
class SyncQueue
  constructor: ->
    @_waiting = []
    @_running = false

  _run_next: ->
    if @_running
      return

    if @_waiting.length > 0
      next = @_waiting[0]
      @_waiting.shift()
      @_running = true
      next()
      return

  request: (cb) ->
    @_waiting.push cb
    @_run_next()

  finish: ->
    @_running = false
    # avoid producing a large call stack if many requests are backed
    # up
    setTimeout (@_run_next.bind @), 0

# @private
#
# An unbounded queue that accepts items one at a time and calls a
# consumer function once for each item while avoiding multiple
# concurrent calls of the consumer function.  The consumer function
# takes two args, an item and a callback.  It should call the callback
# with an argument of null when it's ready for the next item, or an
# error when something went wrong.  (The behavior of the
# ConsumptionQueue isn't very well-defined in the error case right
# now.)
class ConsumptionQueue
  constructor: (@consumer) ->
    @items = []
    @sync_queue = new SyncQueue
    # only one consumer for now

  consume: ->
    @sync_queue.request =>
      if @items.length == 0
        return @sync_queue.finish()

      item = @items.shift()
      @consumer item, (err) =>
        # TODO: handle err...
        throw err if err

        @sync_queue.finish()
        # keep consuming
        @consume()

  push: (item) ->
    @items.push item
    @consume()

  run: ->
    @consume()

