action_gen_m = require '../../datastore/action_gen'
{ActionGenerator, ActionPerformer, ActionTester} = action_gen_m

mock_server_m = require '../../ds_helpers/mock_server'
{MockServer, MockClient} = mock_server_m

{Change, FieldOp} = Dropbox.Datastore.impl

T = Dropbox.Datastore.impl.T

filter_random_n = (l, n) ->
  # randomly select at most n of list l
  return l.slice() if l.length <= n
  ret = []
  num_left = n

  for item, idx in l
    if Math.random() < (num_left / (l.length - idx))
      ret.push item
      num_left--
  return ret

class ChangeGenerator
  copy_value = (v) ->
    return (if v instanceof Array then v.slice() else v)
  field_op_from_value = (v) ->
    val = copy_value v
    arr = if val? then ['P', val] else ['D']
    return (FieldOp.from_array arr)

  get_list_field_ops = (length, possible_values) ->
    ret = []
    for i in [0...length]
      for val in possible_values
        ret.push ['LP', i, val]
    for i in [0...(length + 1)]
      for val in possible_values
        ret.push ['LI', i, val]
    for i in [0...length]
      ret.push ['LD', i]
    for i in [0...length]
      for j in [0...length]
        ret.push ['LM', i, j]
    return (FieldOp.from_array arr for arr in ret)

  constructor: (@ds) ->

  raw_data: ->
    return @ds._managed_datastore.datastore_model.raw_data()

  get_inserts: (tid_rid_pool) ->
    raw_data = @raw_data()

    ret = []
    for tid, rids of tid_rid_pool
      for rid of rids
        continue if raw_data[tid]? and raw_data[tid][rid]?
        # TODO: only insert empty rows for now
        ret.push {type: 'I', tid: tid, rid: rid, data: {}}
    return ret

  get_deletes: ->
    raw_data = @raw_data()
    ret = []
    for tid, rids of raw_data
      if tid is ':info'
        continue
      for rid of rids
        ret.push {type: 'D', tid: tid, rid: rid}
    return ret

  get_updates: (tid, rid, field_pool) ->
    ret = []

    record = @raw_data()[tid][rid]
    assert record?, 'record #{tid}:#{rid} does not exist'
    for field, values of field_pool
      for value in values
        data = {}
        data[field] = value
        ret.push {type: 'U', tid: tid, rid: rid, data: data}
    return ret



class TwoClientState
  @release_put_deltas: (ds) ->
    return ds._managed_datastore.flob_client.client.release_put_deltas()

  @get_rev: (ds) ->
    return ds._managed_datastore.sync_state.get_server_rev()

  @num_unsynced: (ds) ->
    return ds._managed_datastore.sync_state.unsynced_deltas.length

  @apply_ops: (ds, ops) ->
    for op in ops
      table = ds.getTable op.tid
      if op.type == 'I'
        if op.rid?
          table.getOrInsert op.rid, op.data
        else
          table.insert op.data
        continue
      record = table.get op.rid
      if op.type == 'U'
        record.update op.data
        continue
      if op.type == 'D'
        record.deleteRecord()
        continue
      throw new Error "unrecognized op type #{op.type}"

    #ds._managed_datastore.sync_state.finalize()

  constructor: (@local_ds, @remote_ds, @mock_server) ->
    T.assert (TwoClientState.get_rev @local_ds) == 0, => "local ds rev not 0: #{TwoClientState.get_rev @local_ds}"
    T.assert (TwoClientState.get_rev @remote_ds) == 0, => "remote ds rev not 0: #{TwoClientState.get_rev @remote_ds}"

    # cids are 'local' and 'remote'
    @num_actions = 0
    @local_ds.syncStatusChanged.addListener =>
      #console.log 'local change'

  apply_local_ops: (ops, cb) ->
    TwoClientState.apply_ops @local_ds, ops
    return cb null

  # TODO: make this so that you can poll until only partially synced
  _poll_until_synced: (cid, ds, cb) ->
    poll_whether_synced = =>
      num_unsynced = TwoClientState.num_unsynced ds
      local_rev = TwoClientState.get_rev ds
      server_rev = @mock_server.cur_rev()
      if num_unsynced == 0 and local_rev == server_rev
        #console.log cid, 'synced now'
        return cb null

      @mock_server.raise_cap cid, server_rev
      TwoClientState.release_put_deltas ds
      return setTimeout poll_whether_synced, 2
    poll_whether_synced()

  sync_local_ops: (cb) ->
    @_poll_until_synced 'local', @local_ds, (err) =>
      throw err if err
      @_poll_until_synced 'remote', @remote_ds, (err) =>
        throw err if err
        cb null

  commit_remote_ops: (ops, cb) ->
    cur_rev = @mock_server.cur_rev()
    assert cur_rev == (TwoClientState.get_rev @remote_ds), "cur_rev doesn't match remote_ds rev"
    TwoClientState.apply_ops @remote_ds, ops
    @_poll_until_synced 'remote', @remote_ds, (err) =>
      return cb err

  do_action: (action, cb) ->
    @num_actions++
    switch action.type
      when 'apply-local'
        return @apply_local_ops action.ops, cb
      when 'sync-local'
        return @sync_local_ops cb
      when 'commit-remote'
        return @commit_remote_ops action.ops, cb
      when 'set-local-res-rule'
        table = @local_ds.getTable action.tid
        table.setResolutionRule action.field, action.rule
        return cb null
      else
        throw new Error "Unrecognized action type #{action.type}"


class TwoClientPerformer extends ActionPerformer
  THE_DSID = '_mock_dsid_'

  constructor: ->

  initial_state: (cb) ->
    # TODO: we may get stray logs from timeouts from the previous test

    #console.log '-- Initializing new state --'
    mock_server = new MockServer
    local_client = new MockClient 'local', mock_server
    remote_client = new MockClient 'remote', mock_server

    local_mgr = new Dropbox.Datastore.DatastoreManager local_client
    remote_mgr = new Dropbox.Datastore.DatastoreManager remote_client

    local_mgr.deleteDatastore THE_DSID, (err) =>
      return cb err if err
      remote_mgr.openDatastore THE_DSID, (err, remote_ds) =>
        #remote_ds._evt_mgr.unregister_all() # prevents autosync

        # TODO: against the real server, actually have to wait for the
        # change to propagate, probably
        local_mgr.openDatastore THE_DSID, (err, local_ds) =>

          new_state = new TwoClientState local_ds, remote_ds, mock_server
          return cb null, new_state

  apply: (state, action, cb) ->
    state.do_action action, cb


class TwoClientGenerator extends ActionGenerator
  get_actions: (state) ->
    local_changes = new ChangeGenerator state.local_ds
    remote_changes = new ChangeGenerator state.remote_ds

    field_map =
      'f0': [null, 1, 2]
      'f1': [[3], 'a', (Dropbox.Datastore.int64 45)]

    local_list = []
    local_list = local_list.concat (local_changes.get_inserts {'t': {'r0': true, 'r1': true}})
    local_list = local_list.concat local_changes.get_deletes()
    local_table = state.local_ds.getTable 't'
    for record in (local_table.query {})
      local_list = local_list.concat (local_changes.get_updates 't', record.getId(), field_map)

    remote_list = []
    remote_list = remote_list.concat (remote_changes.get_inserts {'t': {'r0': true, 'r1': true}})
    remote_list = remote_list.concat remote_changes.get_deletes()
    remote_table = state.remote_ds.getTable 't'
    for record in (remote_table.query {})
      remote_list = remote_list.concat (remote_changes.get_updates 't', record.getId(), field_map)

    ret = [{type: 'sync-local'}]
    for change in (filter_random_n local_list, 1)
      ret.push {type: 'apply-local', ops: [change]}
    for change in (filter_random_n remote_list, 1)
      ret.push {type: 'commit-remote', ops: [change]}

    return ret


# class TwoClientSumGenerator extends ActionGenerator
#   INITIAL_ACTIONS = [
#     {
#       type: 'commit-remote'
#       ops: [{type: 'I', tid: 't', data: {}}]
#     },
#     {
#       type: 'sync-local'
#     },
#     {
#       type: 'set-local-res-rule'
#       tid: 't'
#       field: 'f'
#       rule: 'sum'
#     }
#   ]

#   get_actions: (state) ->
#     if state.num_actions < INITIAL_ACTIONS.length
#       return [INITIAL_ACTIONS[state.num_actions]]

#     local_changes = new ChangeGenerator state.local_ds
#     remote_changes = new ChangeGenerator state.remote_ds

#     ret = []
#     ret.push {
#       type: 'apply-local'
#       ops: [{type: 'I', tid: 't', data: {}}]
#       }
#     ret.push {
#       type: 'sync-local'
#       }
#     ret.push {
#       type: 'commit-remote'
#       ops: [{type: 'I', tid: 't', data: {}}]
#       }

#     return ret


describe 'Datastore Sync Tests', ->

  it 'client doesn\'t hang under basic conditions', (done) ->
    this.timeout 3000

    mock_server = new MockServer
    local_client = new MockClient 'local', mock_server
    remote_client = new MockClient 'remote', mock_server

    opts =
      report_every: 1000000
    performer = new TwoClientPerformer local_client, remote_client, mock_server
    generator = new TwoClientGenerator
    tester = new ActionTester generator, performer, opts

    # limit it to 3 so that test runs faster
    depth = 3 #6
    tester.run depth, =>
      done()

#   # TODO: implement this test
#   it.only 'sum test', (done) ->

