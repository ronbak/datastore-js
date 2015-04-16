_next_tick = (fn) -> setTimeout fn, 0
if process?
  if setImmediate?
    _next_tick = setImmediate
  else # for old versions of node
    _next_tick = process.nextTick

_while = (cond, block, cb) ->
  do_iter = ->
    return cb() unless cond()
    block ->
      _next_tick do_iter
  do_iter()

_for = (list, block, cb) ->
  idx = 0
  do_iter = ->
    return cb() unless idx < list.length
    block list[idx], ->
      idx++
      _next_tick do_iter
  do_iter()

class ActionGenerator
  constructor: ->

  get_actions: (state) ->
    throw new Error "abstract"

class ActionPerformer
  constructor: ->

  initial_state: (cb) ->
    throw new Error "abstract"

  generate_state: (actions, cb) ->
    @initial_state (err, state) =>
      return cb err if err

      loop_block = (action, inner_cb) =>
        @apply state, action, (err) =>
          return cb err if err
          inner_cb()

      _for actions, loop_block, =>
        return cb null, state

  apply: (state, action, cb) ->
    throw new Error "abstract"

class ActionTester
  DEFAULT_OPTS =
    report_every: 100

  constructor: (@action_gen, @action_performer, @opts = DEFAULT_OPTS) ->
    @cur_stack = []
    @action_queues = []

  report_error: (err) ->
    console.log "error with actions:", err
    # TODO: this assumes actions are JSON
    for action in @cur_stack
      console.log (JSON.stringify action)
    throw err

  run_action_seq: (actions, cb) ->
    @action_performer.initial_state (err, cur_state) =>
      throw err if err

      loop_block = (action, cont) =>
        @action_performer.apply cur_state, action, (err) =>
          throw err if err
          cont()

      _for actions, loop_block, =>
        console.log "final state:", cur_state
        return cb null

  run: (target_depth, cb) ->
    if process?
      old_listeners = process.listeners 'uncaughtException'
      process.removeAllListeners 'uncaughtException'
      process.on 'uncaughtException', (err) =>
        @report_error err

    @action_performer.initial_state (err, state) =>
      throw err if err
      @_run state, target_depth, (err, tested) =>
        if process?
          for listener in old_listeners
            process.addListener 'uncaughtException', listener
        cb err, tested

  _run: (initial_state, target_depth, cb) ->
    cur_state = initial_state
    cur_depth = 0
    @action_queues.push (@action_gen.get_actions cur_state)
    need_reset = false

    all_tested = []
    record_tested = =>
      all_tested.push @cur_stack.slice()
      if all_tested.length % @opts.report_every == 0
        console.log "#{all_tested.length} cases tested"

    while_cond = =>
      return cur_depth > 0 or @action_queues[cur_depth].length > 0

    while_block = (cont) =>
      if cur_depth >= target_depth or @action_queues[cur_depth].length == 0
        if cur_depth >= target_depth
          record_tested()

        # tested all possible actions here
        cur_depth--
        @cur_stack.pop()
        @action_queues.pop()
        need_reset = true
        return cont()

      next_action = @action_queues[cur_depth].shift()
      @cur_stack.push next_action

      end_block = =>
        next_actions = @action_gen.get_actions cur_state
        if next_actions.length == 0
          record_tested()
        @action_queues.push next_actions
        cur_depth++
        cont()

      if need_reset
        @action_performer.generate_state @cur_stack, (err, new_state) =>
          @report_error err if err
          cur_state = new_state
          end_block()
      else
        @action_performer.apply cur_state, next_action, (err) =>
          @report_error err if err
          end_block()

    _while while_cond, while_block, =>
      return cb null, all_tested

exports.ActionGenerator = ActionGenerator
exports.ActionPerformer = ActionPerformer
exports.ActionTester = ActionTester