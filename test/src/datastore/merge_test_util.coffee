assert = require 'assert'

action_gen_m = require './action_gen'
{ActionGenerator, ActionPerformer, ActionTester} = action_gen_m


clone = Dropbox.Datastore.impl.clone

objects_equal = (x, y) ->
  if typeof x != typeof y
    return false

  if x instanceof Array
    return false if x.length != y.length
    for i in [0...x.length]
      return false unless objects_equal x[i], y[i]
    return true

  if typeof x != 'object'
    return x == y

  for k, v of x
    if k not of y
      return false
    return false unless objects_equal x[i], y[i]
  for k, v of y
    if k not of x
      return false
  return true

class MergeState
  constructor: (num_ops, initial_data, @apply_op, @resolve) ->
    # apply_op(data, op)
    # resolve(ops_list1, ops_list2)

    @num_x_ops = num_ops.x
    @num_y_ops = num_ops.y

    @x = (clone initial_data)
    @y = (clone initial_data)

    @x_ops = []
    @y_ops = []

    # gen a bunch of ops common
    # gen a bunch of ops x
    # gen a bunch of ops y
    # stages: initial, mutate-x, mutate-y, final, done
    @stage_num = 0

  get_stage: ->
    n = @stage_num
    if n < @num_x_ops
      return 'mutate-x'

    n -= @num_x_ops
    if n < @num_y_ops
      return 'mutate-y'

    n -= @num_y_ops
    if n < 1
      return 'final'
    return 'done'

  objects_equal = (o1, o2) ->
    if (typeof o1) != (typeof o2)
      return false

    if typeof o1 != 'object'
      return o1 == o2

    if o1 instanceof Array
      return false unless o2 instanceof Array
      return false unless o1.length == o2.length
      for i in [0...o1.length]
        return false unless objects_equal o1[i], o2[i]
      return true

    # should be a simple object
    for k, v of o1
      if k not of o2
        return false
      if not objects_equal v, o2[k]
        return false
    for k, v of o2
      if k not of o1
        return false
    return true

  assert_consistent: ->
    try
      assert (objects_equal @x, @y)
    catch e
      console.log (JSON.stringify @x)
      console.log (JSON.stringify @y)
      throw e

  sync: -> # syncs x against y
    [new_x, new_y] = @resolve @x_ops, @y_ops

    for op in new_y
      @apply_op @x, op
    for op in new_x
      @apply_op @y, op
    @assert_consistent()

  do_action: (action) ->
    switch action.type
      when 'sync'
        @sync()
      when 'op'
        @push_op action.op, action.target
      else
        throw new Error "unrecognized type #{action.type}"

  push_op: (op, target_name) ->
    target = @[target_name]
    @apply_op target, op

    if target_name == 'x'
      @x_ops.push op
    else
      @y_ops.push op


# only option is opts.report_every for now
make_merge_tester = (num_ops, initial_data, apply_op, resolve, get_possible_ops, opts) ->
  # get_possible_ops(data) returns list of possible ops

  class MergeActionGenerator extends ActionGenerator
    get_actions: (state) ->
      switch state.get_stage()
        when 'mutate-x'
          possible_ops = get_possible_ops state.x
          return ({type: 'op', target: 'x', op: op} for op in possible_ops)
        when 'mutate-y'
          possible_ops = get_possible_ops state.y
          return ({type: 'op', target: 'y', op: op} for op in possible_ops)
        when 'final' then return [{type: 'sync'}]
        when 'done' then return []

  class MergeActionPerformer extends ActionPerformer
    constructor: ->

    initial_state: (cb) ->
      state = new MergeState num_ops, initial_data, apply_op, resolve
      return cb null, state

    apply: (state, action, cb) ->
      try
        state.do_action action
        state.stage_num++
      catch e
        return cb e
      return cb null

  return (new ActionTester (new MergeActionGenerator), (new MergeActionPerformer), opts)


exports.make_merge_tester = make_merge_tester
