# Conflict resolution logic (OT, extended for sum resolution rule).

# @private
class FieldOpTransformer
  swap = (fn) ->
    assert fn?
    (a, b) ->
      [x, y] = fn b, a
      return [y, x]

  TYPE_NAMES = ['null', 'bool', 'num', 'str', 'blob', 'ts', 'list']
  TYPE_RANK = {}
  for type_name, idx in TYPE_NAMES
    TYPE_RANK[type_name] = idx
  get_val_type = (val) ->
    return 'null' unless val?
    return 'bool' if T.is_bool val
    return 'num' if val.I? or T.is_number val
    return 'str' if T.is_string val
    return 'blob' if val.B?
    return 'ts' if val.T?
    return 'list' if T.is_array val
    throw new Error "Unrecognized value #{val}"

  is_wrapped_num = (val) ->
    return (T.is_number val) or val.I?

  coerce_num = (val) ->
    return if val.I? then (parseInt val.I) else val

  is_list_less_than = (l1, l2) ->
    for i in [0...l1.length]
      return false if i >= l2.length
      return true if is_less_than l1[i], l2[i]
      return false if is_less_than l2[i], l1[i]
    return l2.length > l1.length

  @_is_less_than = is_less_than = (val1, val2) ->
    type1 = get_val_type val1
    type2 = get_val_type val2

    if type1 != type2
      return TYPE_RANK[type1] < TYPE_RANK[type2]

    return false if type1 == 'null'
    return (val2 and not val1) if type1 == 'bool'
    if type1 == 'num'
      if val1.I? and val2.I?
        return impl.int64_string_less_than val1.I, val2.I
      return (coerce_num val1) < (coerce_num val2)
    if type1 == 'str'
      return val1 < val2
    if type1 == 'blob'
      return val1.B < val2.B
    if type1 == 'ts'
      return (parseInt val1.T, 10) < (parseInt val2.T, 10)
    if type1 == 'list'
      return is_list_less_than val1, val2
    throw new Error "unknown type #{type1}"

  @_compute_sum = compute_sum = (old_val, val1, val2) ->
    as_int = old_val.I? and val1.I? and val2.I?

    # TODO: precision lost for large ints
    if old_val.I? then old_val = parseInt old_val.I
    if val1.I? then val1 = parseInt val1.I
    if val2.I? then val2 = parseInt val2.I

    # this is not exact
    two_to_63 = 9223372036854775808 # rounds to ..4776000
    two_to_64 = 18446744073709551616 # rounds to ..9552000
    # we need this to handle over/underflow "correctly"
    two_to_64_rounded_down = 18446744073709550000

    d1 = val1 - old_val
    result = val2 + d1
    if as_int
      if result >= two_to_63
        result -= two_to_64_rounded_down
      if result < -two_to_63
        result += two_to_64_rounded_down
      result = {I: ('' + result)}
    return result

  right_wins = (op1, op2) ->
    return [null, op2]
  left_wins = (op1, op2) ->
    return [op1, null]

  OP_TYPES = ['P', 'D', 'LC', 'LP', 'LI', 'LD', 'LM']
  LIST_TYPES = ['LC', 'LP', 'LI', 'LD', 'LM']

  @copy = copy = (op) ->
    return FieldOp.from_array (JSON.parse (JSON.stringify op))

  PRECEDENCE_FN_POOL = # returns 'left' or 'right'
    default: (val1, val2) -> 'right'
    remote: (val1, val2) -> 'right'
    local: (val1, val2) -> 'left'
    min: (val1, val2) -> if (is_less_than val1, val2) then 'left' else 'right'
    max: (val1, val2) -> if (is_less_than val1, val2) then 'right' else 'left'
    # this is used for non-summable cases
    sum: (val1, val2) -> 'right'

  constructor: (@rule_name = 'default') ->
    @precedence = PRECEDENCE_FN_POOL[@rule_name]

    @_transforms = {}
    for op_type in OP_TYPES
      @_transforms[op_type] = {}
    for op_type in ['P', 'D']
      for list_type in LIST_TYPES
        @_transforms[op_type][list_type] = left_wins
        @_transforms[list_type][op_type] = right_wins

    for list_type in LIST_TYPES
      if list_type is 'LC'
        @_transforms['LC']['LC'] = (op1, op2) => [null, null]
      else
        @_transforms['LC'][list_type] = right_wins
        @_transforms[list_type]['LC'] = left_wins

    @_transforms['P']['P'] = (op1, op2) =>
      winner = @precedence op1.value, op2.value
      return (if winner == 'left' then [op1, null] else [null, op2])
    @_transforms['P']['D'] = (op1, op2) =>
      winner = @precedence op1.value, null
      return (if winner == 'left' then [op1, null] else [null, op2])
    @_transforms['D']['P'] = (op1, op2) =>
      winner = @precedence null, op2.value
      return (if winner == 'left' then [op1, null] else [null, op2])
    @_transforms['D']['D'] = (op1, op2) =>
      winner = @precedence null, null
      return (if winner == 'left' then [op1, null] else [null, op2])

    @_transforms['LP']['LP'] = (op1, op2) =>
      if op1.at != op2.at
        return [op1, op2]
      winner = @precedence op1.value, op2.value
      return (if winner == 'left' then [op1, null] else [null, op2])
    @_transforms['LP']['LI'] = (op1, op2) =>
      new_op1 = (copy op1)
      new_op1.at += (if op2.before <= op1.at then 1 else 0)
      return [new_op1, op2]
    @_transforms['LP']['LD'] = (op1, op2) =>
      return [null, op2] if op1.at == op2.at
      new_op1 = (copy op1)
      new_op1.at -= (if op2.at < op1.at then 1 else 0)
      return [new_op1, op2]
    @_transforms['LP']['LM'] = (op1, op2) =>
      new_op1 = copy op1
      if op1.at == op2.from
        new_op1.at = op2.to
      else
        new_op1.at -= (if op2.from < new_op1.at then 1 else 0)
        new_op1.at += (if op2.to <= new_op1.at then 1 else 0)
      return [new_op1, op2]

    @_transforms['LI']['LP'] = swap @_transforms['LP']['LI']
    @_transforms['LI']['LI'] = (op1, op2) =>
      [new_op1, new_op2] = [(copy op1), (copy op2)]
      # left one arbitrarily happens second if same position
      if op1.before < op2.before
        new_op2.before += 1
      else
        new_op1.before += 1
      return [new_op1, new_op2]
    @_transforms['LI']['LD'] = (op1, op2) =>
      [new_op1, new_op2] = [(copy op1), (copy op2)]
      new_op1.before -= (if op2.at < op1.before then 1 else 0)
      new_op2.at += (if op1.before <= op2.at then 1 else 0)
      return [new_op1, new_op2]
    @_transforms['LI']['LM'] = (op1, op2) =>
      [new_op1, new_op2] = [(copy op1), (copy op2)]

      if op1.before == op2.to + 1 and op2.from <= op2.to
          # Special case, need to sort the move first
          return [op1, op2]

      if op1.before == op2.to and op2.from > op2.to
          new_op1.before++
          new_op2.from++
          return [new_op1, new_op2]

      new_ins = (if op2.from < op1.before then op1.before - 1 else op1.before)
      new_op2.from += (if op1.before <= op2.from then 1 else 0)
      # left one happens second if same position
      new_op1.before = (if op2.to < new_ins then new_ins + 1 else new_ins)
      new_op2.to += (if new_ins <= op2.to then 1 else 0)
      return [new_op1, new_op2]

    @_transforms['LD']['LP'] = swap @_transforms['LP']['LD']
    @_transforms['LD']['LI'] = swap @_transforms['LI']['LD']
    @_transforms['LD']['LD'] = (op1, op2) =>
      if op1.at == op2.at
        return [null, null]
      [new_op1, new_op2] = [(copy op1), (copy op2)]
      if op1.at < op2.at
        new_op2.at -= 1
      else
        new_op1.at -= 1
      return [new_op1, new_op2]
    @_transforms['LD']['LM'] = (op1, op2) =>
      if op1.at == op2.from
        new_op1 = copy op1
        new_op1.at = op2.to
        return [new_op1, null]
      [new_op1, new_op2] = [(copy op1), (copy op2)]
      new_op1.at -= (if op2.from < new_op1.at then 1 else 0)
      new_op1.at += (if op2.to <= new_op1.at then 1 else 0)

      new_op2.to += (if new_op2.from < new_op2.to then 1 else 0)
      new_op2.from -= (if op1.at < new_op2.from then 1 else 0)
      new_op2.to -= (if op1.at < new_op2.to then 1 else 0)
      new_op2.to -= (if new_op2.from < new_op2.to then 1 else 0)
      return [new_op1, new_op2]

    @_transforms['LM']['LP'] = swap @_transforms['LP']['LM']
    @_transforms['LM']['LI'] = (op1, op2) =>
      [new_op1, new_op2] = [(copy op1), (copy op2)]

      if op2.before == op1.to + 1 and op1.from <= op1.to
          # Special case, need to sort the move first
          return [op1, op2]

      if op2.before == op1.to and op1.from > op1.to
          new_op1.from++
          new_op1.to++
          return [new_op1, new_op2]

      new_ins = (if op1.from < op2.before then op2.before - 1 else op2.before)
      new_op1.from += (if op2.before <= op1.from then 1 else 0)
      # left one happens second if same position
      new_op2.before = (if op1.to < new_ins then new_ins + 1 else new_ins)
      new_op1.to += (if new_ins <= op1.to then 1 else 0)
      return [new_op1, new_op2]

    @_transforms['LM']['LD'] = swap @_transforms['LD']['LM']
    @_transforms['LM']['LM'] = (op1, op2) =>
      if op1.from == op2.from
        if op1.to == op2.to
          return [null, null]
        if op2.from == op2.to
          return [op1, op2]
        # TODO: arbitrarily pick that right wins for now
        new_op2 = copy op2
        new_op2.from = op1.to
        return [null, new_op2]

      if op1.to == op1.from
        new_op1 = copy op1
        new_op1.from += (op2.to <= op1.from) - (op2.from < op1.from)
        if op1.from == op2.to and op2.from < op2.to
          new_op1.from--
        new_op1.to = new_op1.from
        return [new_op1, op2]

      if op2.to == op2.from
        new_op2 = copy op2
        new_op2.from += (op1.to <= op2.from) - (op1.from < op2.from)
        new_op2.to = new_op2.from
        return [op1, new_op2]

      [new_op1, new_op2] = [(copy op1), (copy op2)]
      if op1.to == op2.to and op1.from > op1.to and op2.from > op2.to
          new_op1.to++
          if op2.from > op1.from
            new_op1.from++
          else
            new_op2.from++
          return [new_op1, new_op2]

      if op1.from == op2.to and op2.from == op1.to and op1.from < op1.to
          new_op2.from--
          new_op1.from++
          return [new_op1, new_op2]

      if op1.from > op1.to and op2.from < op2.to and op2.to+1 == op1.to
          return [op1, op2]

      [new_to1, new_from1] = [op1.to, op1.from]
      new_to1 += (if op1.from < new_to1 then 1 else 0)
      new_to1 -= (if op2.from < new_to1 then 1 else 0)
      new_to1 += (if op2.to < new_to1 then 1 else 0) # not displaced
      new_from1 -= (if op2.from < new_from1 then 1 else 0)
      new_from1 += (if op2.to <= new_from1 then 1 else 0)
      new_to1 -= (if new_from1 < new_to1 then 1 else 0)

      [new_to2, new_from2] = [op2.to, op2.from]
      new_to2 += (if op2.from < new_to2 then 1 else 0)
      new_to2 -= (if op1.from < new_to2 then 1 else 0)
      new_to2 += (if op1.to <= new_to2 then 1 else 0) # displaced
      new_from2 -= (if op1.from < new_from2 then 1 else 0)
      new_from2 += (if op1.to <= new_from2 then 1 else 0)
      new_to2 -= (if new_from2 < new_to2 then 1 else 0)

      [new_op1.to, new_op1.from] = [new_to1, new_from1]
      [new_op2.to, new_op2.from] = [new_to2, new_from2]

      return [new_op1, new_op2]


  # gives a CP1 transform of two field ops
  transform: (op1, op2, old_val = null) ->
    if @rule_name == 'sum' and op1.tag() == 'P' and op2.tag() == 'P'
      old_val ?= {I: '0'}

      if ((is_wrapped_num old_val) and
          (is_wrapped_num op1.value) and
          (is_wrapped_num op2.value))
        new_val = compute_sum old_val, op1.value, op2.value
        new_op1 = new_op2 = FieldOp.from_array ['P', new_val]
        return [new_op1, new_op2, op2.value]

    ret = @_transforms[op1.tag()][op2.tag()] op1, op2
    undo_extra = switch op2.tag()
      when 'P' then op2.value
      when 'D' then null
      else {L: true} # HACK(dropbox): indicates old value used to be a list
    ret.push undo_extra
    return ret

impl.FieldOpTransformer = FieldOpTransformer

# @private
class ChangeTransformer
  TRANSFORMER_POOL = {}
  for rule_name in ['default', 'local', 'remote', 'min', 'max', 'sum']
    TRANSFORMER_POOL[rule_name] = new FieldOpTransformer rule_name

  swap = (fn) ->
    assert fn?
    (a, b) ->
      [x, y] = fn b, a
      return [y, x]

  copy_data = (val) ->
    return (if val instanceof Array then val.slice() else val)

  same_row = (o1, o2) ->
    return (o1.tid == o2.tid and o1.rowid == o2.rowid)

  @is_no_op = (change) ->
    if change.tag() != 'U'
      return false
    for k, field_op of change.updates
      return false
    return true

  @compact = (ops_list) ->
    compacted = []
    for op in ops_list
      unless @is_no_op op
        compacted.push op
    return compacted

  constructor: ->
    # @_transform_rules[tid][field_name] = {FieldOpTransformer}
    @_transform_rules = {}
    @_default_transformer = new FieldOpTransformer

  set_field_transformer: (tid, field_name, rule_name) ->
    @_transform_rules[tid] ?= {}
    @_transform_rules[tid][field_name] = TRANSFORMER_POOL[rule_name]

  get_field_transformer: (tid, field_name) ->
    if tid not of @_transform_rules
      return TRANSFORMER_POOL.default
    return @_transform_rules[tid][field_name] ? @_default_transformer

  transform_ii: (i1, i2) ->
    unless same_row i1, i2
      return [[i1], [i2]]
    make_update = (ins) ->
      updates = {}
      for k, v of ins.fields
        updates[k] = FieldOp.from_array ['P', (copy_data v)]
      chg = Change.from_array ['U', ins.tid, ins.rowid, updates]
      chg.undo_extra = {}
      return chg
    # use same conflict resolution as updates
    u1 = make_update i1
    u2 = make_update i2
    return @transform_uu u1, u2
  transform_iu: (i, u) ->
    unless same_row i, u
      return [[i], [u]]
    assert false, "Couldn't have updated a row that hasn't been inserted yet!"
  transform_id: (i, d) ->
    unless same_row i, d
      return [[i], [d]]
    assert false, "Couldn't have deleted a row that hasn't been inserted yet!"

  transform_ui: swap ChangeTransformer.prototype.transform_iu
  transform_uu: (u1, u2) ->
    unless same_row u1, u2
      return [[u1], [u2]]
    [new_updates1, new_updates2] = [{}, {}]
    new_undo_extra1 = {}

    for k, field_op1 of u1.updates
      if k not of u2.updates
        new_updates1[k] = field_op1
        new_undo_extra1[k] = u1.undo_extra[k] ? null
        continue
      field_op2 = u2.updates[k]

      old_val = u1.undo_extra[k] ? null
      transformer = @get_field_transformer u1.tid, k
      [new_op1, new_op2, undo_extra] = transformer.transform field_op1, field_op2, old_val

      if new_op1?
        new_updates1[k] = new_op1
        new_undo_extra1[k] = undo_extra ? null
      (new_updates2[k] = new_op2) if new_op2?

    for k, field_op2 of u2.updates
      if k not of u1.updates
        new_updates2[k] = field_op2

    new_u1 = Change.from_array ['U', u1.tid, u1.rowid, new_updates1]
    new_u1.undo_extra = new_undo_extra1
    new_u2 = Change.from_array ['U', u2.tid, u2.rowid, new_updates2]
    return [[new_u1], [new_u2]]
  transform_ud: (u, d) ->
    unless same_row u, d
      return [[u], [d]]
    return [[], [d]]

  transform_di: swap ChangeTransformer.prototype.transform_id
  transform_du: swap ChangeTransformer.prototype.transform_ud
  transform_dd: (d1, d2) ->
    unless same_row d1, d2
      return [[d1], [d2]]
    # these two ops should be identical anyway
    return [[], []]

impl.ChangeTransformer = ChangeTransformer

# @private
class DefaultResolver
  constructor: ->
    @_change_transformer = new ChangeTransformer

  add_resolution_rule: (tid, field_name, rule_name) ->
    @_change_transformer.set_field_transformer tid, field_name, rule_name

  _transform_one: (local, server) ->
    change_to_letter = (c) ->
      switch c.tag()
        when 'I' then return 'i'
        when 'U' then return 'u'
        when 'D' then return 'd'
        else throw new Error "unrecognized op type #{c.tag()}"
    trans_fn_name = 'transform_' + (change_to_letter local) + (change_to_letter server)

    [new_local, new_server] = @_change_transformer[trans_fn_name] local, server
    new_local = ChangeTransformer.compact new_local
    new_server = ChangeTransformer.compact new_server
    return [new_local, new_server]

  _transform_list: (local_ops, server_ops) ->
    if local_ops.length == 0
      return [[], server_ops]
    if server_ops.length == 0
      return [local_ops, []]

    # TODO: can make this faster probably, and should also make
    # iterative to avoid stack problems.
    first_local = local_ops[0]
    first_server = server_ops[0]
    [new_local, new_server] = @_transform_one first_local, first_server

    [local_rest, new_server] = @_transform_list (local_ops.slice 1), new_server

    (new_local.push op) for op in local_rest
    [new_local, server_rest] = @_transform_list new_local, (server_ops.slice 1)

    (new_server.push op) for op in server_rest
    return [new_local, new_server]

  _resolve: (local_op_batches, server_ops) ->
    ops_to_apply = server_ops.slice()
    new_local_batches = []

    for op_batch in local_op_batches
      [local_batch, ops_to_apply] = @_transform_list op_batch, ops_to_apply
      new_local_batches.push local_batch

    return [new_local_batches, ops_to_apply]


  # returns [rebased_deltas, affected_records]
  # affected_records[tid][rid] = true
  resolve: (local_deltas, server_deltas) ->
    local_change_batches = []
    for delta in local_deltas
      batch = []
      for change, idx in delta.changes
        change_cpy = Change.from_array (JSON.parse (JSON.stringify change))
        # HACK(dropbox): add undo_extra here for purposes of conflict resolution
        change_cpy.undo_extra = impl.clone delta.undo_extras[idx]
        batch.push change_cpy
      local_change_batches.push batch

    server_changes = []
    for delta in server_deltas
      for c in delta.changes
        server_changes.push c

    [new_local_change_batches, changes_to_apply] = @_resolve local_change_batches, server_changes
    ret = []
    for change_batch, idx in new_local_change_batches
      # TODO: undo extras (for now they are generated by the ManagedDatastore)
      undo_extras = (null for change in change_batch)

      for change in change_batch
        # HACK(dropbox): get rid of undo_extra here that's only used
        # for conflict resolution
        delete change.undo_extra

      if change_batch.length > 0
        ret.push (new LocalDelta change_batch, undo_extras)

    # TODO: we can also just return exactly what changed here
    affected_records = {} # affected_records[tid][rid] = true
    for c in changes_to_apply
      unless c.tid of affected_records
        affected_records[c.tid] = {}
      affected_records[c.tid][c.rowid] = true

    return {
      rebased_deltas: ret
      affected_records: affected_records
      }

impl.DefaultResolver = DefaultResolver
