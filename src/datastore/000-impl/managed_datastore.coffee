# Implementation of datastore logic.  "managed" refers to obj_manager.

# @private
class LocalDelta
  _make_inverse = (change, undo_extra) ->
    tag = null
    data = null
    switch change.tag()
      when 'I'
        tag = 'D'
      when 'U'
        tag = 'U'
        data = {}
        for k, v of undo_extra
          if not v?
            data[k] = ['D']
          else
            data[k] = ['P', v]
      when 'D'
        tag = 'I'
        data = (impl.clone undo_extra)
      else throw new Error "Unknown change tag: #{change.tag()}"

    arr = [tag, change.tid, change.rowid]
    arr.push data if data?
    return (Change.from_array arr)

  # @private
  constructor: (new_changes, new_undo_extras) ->
    T.assert new_changes.length == new_undo_extras.length,
      -> "#{new_changes.length} changes, #{new_undo_extras.length} undo_extras"
    # @changes and @undo_extras are parallel arrays -- @undo_extras[i]
    # is the undo information for @changes[i].
    @changes = []
    @undo_extras = []
    # index (in @changes) of the last change that satisfies
    # @_is_simple_mtime_update.  Null if there is no such operation,
    # or if the last operation affecting the mtime field is not of
    # that form (e.g. if there's an op deleting the info record).
    @_last_simple_mtime_update = null
    for i in [0...new_changes.length]
      @add_change new_changes[i], new_undo_extras[i]

  # @private
  # True iff `change` has the form ```['U', ':info', 'info', { mtime:
  # ['P', <some value>] }]```.  Within the same delta, a change of this
  # form can be dropped if followed by another change of this form,
  # with no changes in between that also affect mtime and are not of
  # this form.
  @_is_simple_mtime_update: (change) ->
    switch change.tag()
      when 'I', 'D'
        return false
      when 'U'
        if not ((change.tid == ':info') and (change.rowid == 'info'))
          return false
        keys = Object.keys change.updates
        if keys.length != 1
          return false
        if keys[0] != 'mtime'
          return false
        field_op = change.updates['mtime']
        switch field_op.tag()
          when 'P'
            return true
          when 'D', 'LC', 'LP', 'LI', 'LD', 'LM'
            return false
          else throw new Error "Unknown field op: #{field_op.tag()}"
      else throw new Error "Unknown change tag: #{change.tag()}"

  # @private
  # True iff `change` updates mtime or inserts/deletes the entire info
  # record.  (Inserting/deleting the record counts as affecting the
  # mtime regardless of whether the mtime field is present.)
  @_affects_mtime: (change) ->
    if not ((change.tid == ':info') and (change.rowid == 'info'))
      return false
    switch change.tag()
      when 'I', 'D'
        return true
      when 'U'
        if 'mtime' of change.updates
          return true
        else
          return false
      else throw new Error "Unknown change tag: #{change.tag()}"

  # @private
  # Adds the change to the delta, compressing away redundant mtime
  # updates (at least those that are simple).
  add_change: (change, undo_extra) ->
    @changes.push change
    @undo_extras.push undo_extra
    if LocalDelta._affects_mtime change
      if LocalDelta._is_simple_mtime_update change
        if @_last_simple_mtime_update?
          @changes.splice @_last_simple_mtime_update, 1
          @undo_extras.splice @_last_simple_mtime_update, 1
        @_last_simple_mtime_update = @changes.length - 1
      else
        @_last_simple_mtime_update = null
    undefined

  # @private
  inverse_changes: ->
    ret = []
    for change, idx in @changes
      ret.push (_make_inverse change, @undo_extras[idx])
    ret.reverse()
    return ret

impl.LocalDelta = LocalDelta

NONCE_LENGTH = 10
# The impl.value_size of a nonce.  We don't want to call
# impl.value_size to compute this at load-time because load-order
# dependencies are a headache.
NONCE_VALUE_SIZE = NONCE_LENGTH

impl.make_nonce = () ->
  impl.randomWeb64String NONCE_LENGTH


impl.value_size = (x) ->
  if T.is_string x
    return Dropbox.Util.countUtf8Bytes x
  else if T.is_bool x
    return 0
  else if T.is_number x
    return 0
  else if T.is_array x
    size = Dropbox.Datastore.List.BASE_ITEM_SIZE * x.length
    for y in x
      size += impl.value_size y
    return size
  else
    if typeof x != 'object'
      throw new Error "Unexpected value: #{x}"
    if x.I?
      return 0
    else if x.N?
      return 0
    else if x.B?
      return Math.ceil (x.B.length * 3 / 4)
    else if x.T?
      return 0
    else
      throw new Error "Unexpected object: #{JSON.stringify x}"


# Size tracking: per-record size cache makes record deletion fast
# (don't have to compute the size of the deleted record), and allows
# us to check per-record size limits without recomputing.  Replacing a
# list still takes linear time in the length of the list... perhaps we
# should fix that by caching list sizes as well.  Same for string
# lengths, which we have to count every time.
#
# One could argue that linear-time deletion is OK because the cost of
# deletion still is <= the cost of creation.  But it doesn't feel
# right to make deletions expensive _just_ for size tracking.

impl.size_difference_for_field_op = (record, field_name, field_op) ->
  current_value = record.get field_name
  switch field_op.tag()
    when 'P'
      new_value = field_op.value
      if not current_value?
        Dropbox.Datastore.Record.BASE_FIELD_SIZE + impl.value_size new_value
      else
        (impl.value_size new_value) - (impl.value_size current_value)
    when 'D'
      if current_value?
        -(Dropbox.Datastore.Record.BASE_FIELD_SIZE + impl.value_size current_value)
      else
        0
    when 'LC'
      assert (not current_value?), "can't create list for field that already exists"
      Dropbox.Datastore.Record.BASE_FIELD_SIZE
    when 'LP'
      assert (T.is_array current_value), "LP on non-list"
      assert (0 <= field_op.at < current_value.length), "bad index for LP"
      ((impl.value_size field_op.value) - (impl.value_size current_value[field_op.at]))
    when 'LI'
      ((if current_value? then 0 else Dropbox.Datastore.Record.BASE_FIELD_SIZE) +
       Dropbox.Datastore.List.BASE_ITEM_SIZE + impl.value_size field_op.value)
    when 'LD'
      assert (T.is_array current_value), "LD on non-list"
      assert (0 <= field_op.at < current_value.length), "bad index for LD"
      -(Dropbox.Datastore.List.BASE_ITEM_SIZE + impl.value_size current_value[field_op.at])
    when 'LM'
      0
    else throw new Error "unexpected field op type #{field_op.tag()}"

impl.size_difference_for_change = (datastore, change) ->
  size_difference = switch change.tag()
    when 'I'
      size = Dropbox.Datastore.Record.BASE_RECORD_SIZE
      for field_name, value of change.fields
        size += Dropbox.Datastore.Record.BASE_FIELD_SIZE + impl.value_size value
      size
    when 'U'
      record = datastore.get_record change.tid, change.rowid
      T.assert record?, -> "record not found: #{JSON.stringify change}"
      total = 0
      for field_name, field_op of change.updates
        total += impl.size_difference_for_field_op record, field_name, field_op
      total
    when 'D'
      -(datastore.get_record change.tid, change.rowid)._size
    else throw new Error "unrecognized tag #{change.tag()}"
  size_difference


# TODO: figure out where exactly the boundaries between these model
# classes and the API classes are

# @private
class RecordModel
  constructor: (@_tid, @_rid, fields = {}) ->
    # map of field names to values
    @_fields = {}
    @_size = Dropbox.Datastore.Record.BASE_RECORD_SIZE
    for field_name, value of fields
      @_fields[field_name] = (impl.clone value)
      @_size += Dropbox.Datastore.Record.BASE_FIELD_SIZE + impl.value_size value
    undefined

  get: (field_name) ->
    @_fields[field_name]

  get_all: ->
    @_fields

  # only used during rollback on error.  TODO: express undo_extra as a
  # field_op and remove this method
  put: (field_name, value) ->
    if value?
      @_fields[field_name] = (impl.clone value)
    else
      delete @_fields[field_name]
    undefined

  apply_field_op: (field_name, field_op) ->
    field = @_fields[field_name]
    switch field_op.tag()
      when 'P'
        @_fields[field_name] = (impl.clone field_op.value)
      when 'D'
        delete @_fields[field_name]
      when 'LC'
        assert (not field?), "can't create list for field that already exists"
        @_fields[field_name] = []
      when 'LP'
        assert (T.is_array field), "LP on non-list"
        assert (0 <= field_op.at < field.length), "bad index for LP"
        field[field_op.at] = (impl.clone field_op.value)
      when 'LI'
        if field?
          assert (T.is_array field), "LI on non-list"
          assert (0 <= field_op.before <= field.length), "bad index for LI"
          field.splice field_op.before, 0, (impl.clone field_op.value)
        else
          assert field_op.before == 0, "bad index for LI on nonexistent field"
          @_fields[field_name] = [(impl.clone field_op.value)]
      when 'LD'
        assert (T.is_array field), "LD on non-list"
        assert (0 <= field_op.at < field.length), "bad index for LD"
        field.splice field_op.at, 1
      when 'LM'
        assert (T.is_array field), "LM on non-list"
        assert (0 <= field_op.from < field.length), "bad from index for LM"
        assert (0 <= field_op.to < field.length), "bad to index for LM"
        val = field[field_op.from]
        field.splice field_op.from, 1
        field.splice field_op.to, 0, val
      else throw new Error "unexpected field op type #{field_op.tag()}"
    undefined

  size: ->
    @_size

# @private
class TableModel
  constructor: () ->
    # map of record ids to records
    @_records = {}
    undefined

  get: (rid) ->
    @_records[rid]

  # null record means remove
  put: (rid, record) ->
    if record?
      @_records[rid] = record
    else
      delete @_records[rid]
    undefined

  has: (rid) ->
    @_records[rid]?

  is_empty: ->
    for x of @_records
      return false
    return true

  list_record_ids: ->
    (rid for rid of @_records)


# @private
class DatastoreModel
  @from_get_snapshot_resp = (resp) ->
    out = {}
    for row in resp.rows
      out[row.tid] or= {}
      out[row.tid][row.rowid] = row.data
    return new DatastoreModel false, out

  # if we want to eventually transition to datastores that don't fit
  # into memory, this can be treated as an in-memory "overlay" (may
  # need to make some calls async)
  #
  # data is a map of table ids to maps of record ids to maps of field
  # names to values.
  constructor: (enforce_limit_during_construction, data) ->
    # map of table ids to TableModel instances
    @_tables = {}
    @_record_count = 0
    @_size = Dropbox.Datastore.BASE_DATASTORE_SIZE

    for tid, records of data
      table = @_get_table tid
      for rid, fields of records
        record = new RecordModel tid, rid, fields
        @_check_record_size enforce_limit_during_construction, tid, rid, record._size
        table.put rid, record
        @_record_count += 1
        @_size += record._size
    @_check_datastore_size enforce_limit_during_construction, @_size
    # Fields of the info record that have local changes that haven't
    # been accepted by the server yet.  This is a set, implemented as
    # a map where every value that is present maps to true.
    @_changedInfoFields = {}
    undefined

  _size_limit_exceeded: (enforce_limit, msg) ->
    if enforce_limit
      err = new Error msg
      err.code = 'SIZE_LIMIT_EXCEEDED'
      throw err
    else
      console.warn msg
      undefined

  _check_record_size: (enforce_limit, tid, rid, size) ->
    if size > Dropbox.Datastore.Record.RECORD_SIZE_LIMIT
      @_size_limit_exceeded enforce_limit, "Record (#{tid}, #{rid}) too large: #{size} bytes"
    undefined

  _check_datastore_size: (enforce_limit, size) ->
    if size > Dropbox.Datastore.DATASTORE_SIZE_LIMIT
      @_size_limit_exceeded enforce_limit, "Datastore too large: #{size} bytes"
    undefined

  # XXX decide what to do with this
  _TEST_calculate_size_from_scratch: ->
    size = 0
    for tid, table of @_tables
      for rid in table.list_record_ids()
        record = table.get rid
        size += Dropbox.Datastore.Record.BASE_RECORD_SIZE
        for fname, value of record.get_all()
          size += Dropbox.Datastore.Record.BASE_FIELD_SIZE + impl.value_size value
    size

  raw_data: ->
    out = {}
    for tid, table of @_tables
      out[tid] = {}
      for rid in table.list_record_ids()
        out[tid][rid] = (impl.clone (table.get rid).get_all())
    out

  get_record: (tid, rid) ->
    @_tables[tid]?.get rid

  clearInfoFields: ->
    @_changedInfoFields = {}

  updateInfoFieldsFromChange: (change) ->
    T.assert (change.tid == ':info'), -> "updateInfoField: table must be :info, got #{change.tid}"
    T.assert (change.rowid == 'info'), -> "updateInfoField: row must be info, got #{change.rowid}"
    switch change.tag()
      when 'I'
        for field_name, insert of change.fields
          @_changedInfoFields[field_name] = true
      when 'U'
        for field_name, update of change.updates
          @_changedInfoFields[field_name] = true
      when 'D'
        # record was deleted, so set everything to null
        infoRecord = @get_record ':info', 'info'
        if infoRecord?
          for field_name of infoRecord.get_all()
            @_changedInfoFields[field_name] = true
      else throw new Error "Unknown change tag: #{change.tag()}"

  # Returns a copy of `info`, updated with changes that haven't been
  # accepted by the server yet.  Values in `info` are raw DS values.
  # No effect on the datastore.
  updateDatastoreInfo: (info) ->
    info = impl.clone (info || {})
    local_fields = (@query ':info', 'info') || {}
    for name of @_changedInfoFields
      if name of local_fields
        info[name] = local_fields[name]
      else
        delete info[name]
    return info

  getLocalInfoData: ->
    return impl.clone (@query ':info', 'info') || {}

  apply_change: (enforce_limit, change) ->
    amount = impl.size_difference_for_change @, change
    if amount >= 0
      @_check_datastore_size enforce_limit, (@_size + amount)

    if change.tid == ':info' and change.rowid == 'info'
      @updateInfoFieldsFromChange change

    switch change.tag()
      when 'I'
        # This exploits that the size of the new record == amount.
        @_check_record_size enforce_limit, change.tid, change.rowid, amount
        @_record_count += 1
        undo_extra = @_apply_insert change
      when 'U'
        record = @get_record change.tid, change.rowid
        T.assert record?, -> "apply_change: record does not exist: #{JSON.stringify change}"
        if amount >= 0
          @_check_record_size enforce_limit, change.tid, change.rowid, record._size + amount
        undo_extra = @_apply_update record, change
        record._size += amount
      when 'D'
        @_record_count -= 1
        undo_extra = @_apply_delete change
      else throw new Error "unrecognized tag #{change.tag()}"

    @_size += amount
    undo_extra

  _get_table: (tid) ->
    unless @_tables[tid]?
      @_tables[tid] = new TableModel
    return @_tables[tid]

  _apply_insert: (change) ->
    table = @_get_table change.tid
    T.assert (not table.has change.rowid), -> "_apply_insert: record already exists: #{JSON.stringify change}"
    record = new RecordModel change.tid, change.rowid, change.fields
    table.put change.rowid, record
    return null

  _apply_update: (record, change) ->
    old_data = {}
    try
      for field_name, field_op of change.updates
        old_value = (impl.clone ((record.get field_name) ? null))
        record.apply_field_op field_name, field_op
        old_data[field_name] = old_value
    catch err
      for field_name, old_value of old_data
        record.put false, field_name, old_value
      throw err
    return old_data

  _apply_delete: (change) ->
    table = @_get_table change.tid
    T.assert (table.has change.rowid), -> "_apply_delete: record does not exist: #{JSON.stringify change}"
    record = table.get change.rowid
    old_data = (impl.clone record.get_all())
    table.put change.rowid, null

    if table.is_empty()
      delete @_tables[change.tid]

    return old_data

  query: (tid, rid) ->
    table = @_tables[tid]
    return null if not table?
    record = table.get rid
    return null if not record?
    return (impl.clone record.get_all())

  list_tables: ->
    ret = (tid for tid of @_tables)
    ret.sort()
    return ret

  list_rows_for_table: (tid) ->
    table = @_tables[tid]
    return [] unless table?
    ret = table.list_record_ids()
    ret.sort()
    return ret

  record_count: ->
    @_record_count

  size: ->
    @_size


# @private
class ManagedDatastore

  @fresh_managed_datastore = (dbid, handle, role, datastore_model, rev, resolver, flob_client, dslist_listener) ->
    sync_state = new SyncState rev
    return (new ManagedDatastore dbid, handle, role, datastore_model, resolver, sync_state, flob_client, dslist_listener)

  # TODO: make fields private
  constructor: (@dbid, @handle, @role, @datastore_model, @resolver, @sync_state, @flob_client, @_dslist_listener) ->
    @syncStateChanged = new Dropbox.Util.EventSource
    @_deleted = false
    @_open = true
    @_put_delta_queue = new SyncQueue
    # HACK(dropbox): in tests, we set this to false to disable mtime updates
    @_update_mtime_on_change = true

  get_dsid: ->
    @dbid

  get_handle: ->
    @handle

  get_effective_role: ->
    @role

  is_deleted: ->
    return @_deleted

  mark_deleted: ->
    # TODO: check _deleted in a bunch of places...
    @_deleted = true

  open: ->
    if @_open
      throw new Error "Attempt to open datastore multiple times"
    @_open = true

  close: ->
    if not @_open
      throw new Error "Attempt to close datastore multiple times"
    @_open = false

  is_closed: ->
    not @_open

  # clears @_changedInfoFields and updates it with deltas that have not been synced.
  # This should be called right after syncing
  _updateInfoFieldsAfterSync: ->
    @datastore_model.clearInfoFields()
    for delta in @sync_state.unsynced_deltas
      for change in delta.changes
        if change.tid == ':info' and change.rowid == 'info'
          @datastore_model.updateInfoFieldsFromChange change

  # Whether the given change affects the datastore list in a way
  # that's worth notifying about.  Must be called with the datastore
  # in the state where the change is about to be applied.
  _should_notify_dslist_listener_for: (change) ->
    # Whether the given fields_or_updates have a field other than "mtime".
    has_fields_other_than_mtime = (fields_or_updates) ->
      for name of fields_or_updates
        if name != 'mtime'
          return true
      return false

    if not (change.tid == ':info' and change.rowid == 'info')
      return false
    switch change.tag()
      when 'D'
        record = @datastore_model.get_record change.tid, change.rowid
        if not record?
          throw new Error "Record not found: #{change.tid} #{change.rowid}"
        return has_fields_other_than_mtime record.get_all()
      when 'U'
        return has_fields_other_than_mtime change.updates
      when 'I'
        return has_fields_other_than_mtime change.fields
      else
        throw new Error "unknown change tag: #{change.tag()}"

  _rollback_unsynced_deltas: (compute_affected_records) ->
    affected_records = {}
    reversed_deltas = @sync_state.unsynced_deltas.slice().reverse()
    for delta in reversed_deltas
      changes = delta.inverse_changes()
      for change in changes
        @datastore_model.apply_change false, change
      # Update affected_records only if requested
      if compute_affected_records
        for c in changes
          unless c.tid of affected_records
            affected_records[c.tid] = {}
          affected_records[c.tid][c.rowid] = true
    return affected_records

  _do_sync: ->
    server_deltas = @sync_state.get_server_deltas()
    if server_deltas.length == 0
      return {}

    res_info = @resolver.resolve @sync_state.unsynced_deltas, server_deltas

    new_local_deltas = res_info.rebased_deltas
    affected_records = res_info.affected_records
    should_notify_dslist = false

    @_rollback_unsynced_deltas false
    for delta in server_deltas
      for change in delta.changes
        should_notify_dslist |= @_should_notify_dslist_listener_for change
        @datastore_model.apply_change false, change
    for delta in new_local_deltas
      # TODO: temporary hack to generate undo extras
      delta.undo_extras = []
      for change in delta.changes
        should_notify_dslist |= @_should_notify_dslist_listener_for change
        # TODO: figure out how to signal "size limit exceeded after
        # rebase".  This shouldn't happen with the basic built-in
        # rebase rules, though.
        undo_extra = @datastore_model.apply_change false, change
        delta.undo_extras.push undo_extra

    @sync_state.update_unsynced_deltas new_local_deltas
    if should_notify_dslist
      @_dslist_listener()
    return affected_records

  _do_put_delta: ->
    return if @sync_state.delta_pending()

    # NB: this won't commit unless the next commit is finalized first
    delta = @sync_state.get_delta_to_put()
    return unless delta?

    @_put_delta_queue.request =>
      @flob_client.put_delta @handle, delta, (err, resp) =>
        if not err?
          if resp.rev?
            @sync_state.put_succeeded delta
            @syncStateChanged.dispatch null
          if resp.access_denied?
            console.log "Write access denied, reverting pending changes. Reason:", resp.access_denied
            @_affected_records_from_access_denied = @_rollback_unsynced_deltas true
            @sync_state.clear_unsynced_deltas()
            @role = impl.ROLE_VIEWER
            @syncStateChanged.dispatch null
        @_put_delta_queue.finish()

  _apply_and_queue_local_change: (enforce_limit, change) ->
    should_notify_dslist = @_should_notify_dslist_listener_for change
    undo_extra = @datastore_model.apply_change enforce_limit, change
    @sync_state.add_unsynced_change change, undo_extra
    if should_notify_dslist
      @_dslist_listener()
    undefined

  _clock: ->
    new Date()

  _update_mtime: ->
    return null if not @_update_mtime_on_change
    mtime_change = if (@datastore_model.query ':info', 'info')?
      Change.from_array ['U', ':info', 'info', { mtime: ['P', impl.toDsValue(@_clock())] }]
    else
      Change.from_array ['I', ':info', 'info', { mtime: impl.toDsValue(@_clock()) }]
    # We don't enforce the size limit here, making use of the 10%
    # extra that the server gives us.
    @_apply_and_queue_local_change false, mtime_change

  perform_local_change: (change) ->
    @_apply_and_queue_local_change true, change
    @_update_mtime()
    @syncStateChanged.dispatch null

  sync: ->
    affected_records = @_affected_records_from_access_denied
    if affected_records?
      delete @_affected_records_from_access_denied
      return affected_records
    if @has_unfinalized_changes()
      @sync_state.finalize()
    affected_records = @_do_sync()
    @_do_put_delta()
    @_updateInfoFieldsAfterSync()
    return affected_records

  get_outgoing_delta_count: ->
    @sync_state.unsynced_deltas.length

  get_incoming_delta_count: ->
    @sync_state.get_server_deltas().length

  has_unfinalized_changes: ->
    @sync_state.has_unfinalized_changes()

  receive_server_delta: (delta) ->
    @sync_state.receive_server_delta delta
    @syncStateChanged.dispatch null
    undefined

  # TODO: later we can cut down on a lot of these calls probably
  query: (tid, rid) ->
    return @datastore_model.query tid, rid

  list_tables: ->
    return (tid for tid in @datastore_model.list_tables() when tid isnt ':info' and tid isnt impl.ACL_TID)

  list_rows_for_table: (tid) ->
    return (@datastore_model.list_rows_for_table tid)

  get_record_count: ->
    @datastore_model.record_count()

  get_record_size: (tid, rid) ->
    @datastore_model.get_record(tid, rid).size()

  get_size: ->
    @datastore_model.size()


# @private
class SyncState
  # _server_rev is the rev that the server has.
  constructor: (@_server_rev) ->
    T.uint @_server_rev, "_server_rev"
    # pending_delta is the delta in a pending put_delta call.
    @_pending_delta = null
    @_server_deltas = []
    # TODO(dropbox): make private, and expose with sensible API
    @unsynced_deltas = []
    # If true, the last entry in @unsynced_deltas is not ready to be
    # submitted (app has not committed the changes yet).  True also
    # implies that there is at least one entry.
    @_last_unsynced_delta_unfinalized = false

  get_server_rev: ->
    @_server_rev

  is_current: ->
    # i.e. fully up-to-date
    return (@unsynced_deltas.length == 0 and
            @_server_deltas.length == 0)

  get_server_deltas: ->
    @_server_deltas

  add_unsynced_change: (change, undo_extra) ->
    len = @unsynced_deltas.length
    if @_last_unsynced_delta_unfinalized
      @unsynced_deltas[len - 1].add_change change, undo_extra
    else
      @unsynced_deltas.push (new LocalDelta [change], [undo_extra])
      @_last_unsynced_delta_unfinalized = true
    undefined

  # "compacting" deltas means merging all the finalized but unsynced
  # deltas into a single delta.
  _compact_deltas: ->
    assert not @_pending_delta?, "delta pending"
    len = @unsynced_deltas.length
    return if len <= 1

    # If the last delta is unfinalized, we pop it off, then push it
    # back on when we're done compacting.
    unfinalized_delta = if @_last_unsynced_delta_unfinalized
      @unsynced_deltas.pop()
    else
      null

    changes = []
    undo_extras = []
    for delta in @unsynced_deltas
      (changes.push c) for c in delta.changes
      (undo_extras.push u) for u in delta.undo_extras
    @unsynced_deltas = [(new LocalDelta changes, undo_extras)]
    if unfinalized_delta?
      @unsynced_deltas.push unfinalized_delta
    undefined

  get_delta_to_put: ->
    assert not @_pending_delta?, "delta pending"
    len = @unsynced_deltas.length
    return null if len == 0 or (len == 1 and @_last_unsynced_delta_unfinalized)

    @_compact_deltas()
    if @_last_unsynced_delta_unfinalized
      assert @unsynced_deltas.length > 1, "the only delta is unfinalized"
    next_delta = @unsynced_deltas[0]

    # TODO: this up to the caller
    @_pending_delta = new Delta
      changes: next_delta.changes.slice()
      nonce: impl.make_nonce()
      rev: @_server_rev
    return @_pending_delta

  delta_pending: ->
    return @_pending_delta?

  has_unfinalized_changes: ->
    @_last_unsynced_delta_unfinalized

  finalize: ->
    @_last_unsynced_delta_unfinalized = false
    undefined

  # called at the end of rebasing
  update_unsynced_deltas: (new_deltas) ->
    @unsynced_deltas = new_deltas
    @_server_rev += @_server_deltas.length
    @_server_deltas = []

  _is_our_pending: (server_delta) ->
    return @_pending_delta? and @_pending_delta.nonce == server_delta.nonce

  # Called when we receive acknowledgment that the server accepted a
  # put_delta request from us, either in the response to put_delta, or
  # by noticing our own delta in the /await stream.
  _ack: (server_delta) ->
    assert (@_is_our_pending server_delta), "not ours"
    # If we have pending _server_deltas, that means we're behind the
    # server's current rev, so the server wouldn't have accepted a
    # put_delta from us.
    assert @_server_deltas.length == 0, "server deltas exist"

    @_pending_delta = null
    @unsynced_deltas.shift()
    @_server_rev++

  put_succeeded: (delta) ->
    if @_is_our_pending delta
      @_ack delta

  # A put_delta returned access_denied. Clear all unsynced deltas.
  # This is instead of put_succeeded, and much more serious.
  # The caller must already have rolled back all these changes.
  # The incoming queue is unaffected.
  clear_unsynced_deltas: ->
    @_pending_delta = null
    @unsynced_deltas = []
    @_last_unsynced_delta_unfinalized = false

  receive_server_delta: (delta) ->
    len = @_server_deltas.length
    expected_rev = if len > 0
      @_server_deltas[len - 1].rev + 1
    else
      @_server_rev

    assert delta.rev <= expected_rev,
      "was expecting rev #{expected_rev}, but got #{delta.rev} instead!"

    if delta.rev < expected_rev
      # This is expected if this was our delta, and we already know it
      # was accepted because the put succeeded.
      #console.warn "received old delta!", delta
      return

    if @_is_our_pending delta
      @_ack delta
    else
      @_server_deltas.push delta
      @_pending_delta = null
    undefined

impl.DatastoreModel = DatastoreModel
