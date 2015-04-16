# ObjectManager is an implementation class that backs
# DatastoreManager.  The distinction and interface between the two is
# not particularly well-defined; grown rather than designed.
#
# ObjectManager also manages long-polling (through FakeUpdateManager
# and PendingPoll) since that is shared between datastores.

# @private
class PendingPoll

  # TODO: reduce coupling
  constructor: (@update_manager) ->
    @cancelled = false
    @cancel_fn = null

  cancel: ->
    # TODO: cancel the actual XHR; this only cancels the retries
    if @cancel_fn?
      @cancel_fn()
    @cancelled = true

  poll: ->
    do_one_poll = =>
      return if @cancelled

      # defensively make a copy since _handle_version_map can change
      # from under us
      rev_map = impl.clone @update_manager._handle_version_map

      # HACK(dropbox): for now, we always poll for the dslist, even if
      # nobody is interested.
      #if T.is_empty rev_map
      #  return

      @cancel_fn = @update_manager.flob_client.await rev_map, @update_manager._last_dslist_token, (err, resp) =>
        @cancel_fn = null
        #assert not @cancelled

        if err
          if err.status == 0
            # means we're offline
            console.log "await deltas failed (offline):", err
            return setTimeout do_one_poll, 10000
          if err.status and (500 <= err.status <= 599)
            console.log "server error:", err
            return setTimeout do_one_poll, 2000
          # TODO: what are other possible errors, and how should we handle them?
          #
          # we might get a 4XX if we accidentally await for a
          # datastore we don't have; in that case do we need a
          # mechanism for dropping the offending datastore from our
          # rev map?
          console.error "Got error in longpoll:", err
          return setTimeout do_one_poll, 10000

        if resp.get_deltas?
          for handle, data of resp.get_deltas.deltas
            #console.log "#{data.deltas.length} deltas for handle #{handle}"
            if data.notfound?
              @update_manager._data_queue.push {handle: handle, notfound: data.notfound}
              delete @update_manager._handle_version_map[handle]
            else if data.deltas?
              if data.role?
                @update_manager._data_queue.push {handle: handle, role: data.role}
              for delta in data.deltas
                @update_manager._data_queue.push {handle: handle, delta: delta}
              next_version = rev_map[handle] + data.deltas.length
              # need to check this in case someone advanced (or deleted)
              # the version while we were awaiting deltas
              cur_version = @update_manager._handle_version_map[handle]
              if cur_version?
                @update_manager._handle_version_map[handle] = Math.max cur_version, next_version

        if resp.list_datastores?
          @update_manager._last_dslist_token = resp.list_datastores.token
          @update_manager._data_queue.push {dslist: resp.list_datastores}

        setTimeout do_one_poll, 0

    do_one_poll()

# @private
# TODO: rename
class FakeUpdateManager
  constructor: (@flob_client) ->
    @_data_queue = null
    @_handle_version_map = {}
    @_last_dslist_token = "."

    @_pending_poll = null
    @_running = false

  run: (update_consumer) ->
    @_data_queue = (new ConsumptionQueue update_consumer)
    #console.info "FakeUpdateManager.run() starting longpoll"
    @_running = true

    # HACK(dropbox): always poll for dslist
    @_do_longpoll()

  stop: ->
    if @_pending_poll
      @_pending_poll.cancel()

  # If we're already polling at a later version, this is a
  # no-op.
  #
  # TODO: figure out what behavior should go here. Maybe we should
  # actually pick the earlier version to poll from...
  add_poll: (handle, version) ->
    assert @_running, "update manager is not running"

    cur_version = @_handle_version_map[handle]
    new_version = version
    if cur_version?
      new_version = Math.max version, cur_version
    @_handle_version_map[handle] = new_version

    @_do_longpoll()

  remove_poll: (handle) ->
    assert @_running, "update manager is not running"
    return unless handle of @_handle_version_map

    delete @_handle_version_map[handle]
    # cancel the old poll so we don't get further info about this
    # handle
    @_do_longpoll()

  _do_longpoll: ->
    assert @_running, "update manager is not running"
    if @_pending_poll
      @_pending_poll.cancel()
      @_pending_poll = null

    @_pending_poll = new PendingPoll @
    @_pending_poll.poll()


# @private
class ObjectManager
  constructor: (@update_manager, @flob_client, @_dslist_listener_server, @_dslist_listener_local) ->
    @update_manager.run (@_handle_server_update.bind @)

    # map of dsid to managed_datastore instances
    @_cached_objects = {}
    @_handle_to_dsid_map = {}

    # TODO(dropbox): disallow opening a datastore multiple times, and
    # support closing datastores.

  # call to clean up the datastores we're tracking; assumes we will
  # never use this object manager again
  destroy: ->
    for dsid of @_cached_objects
      @_cached_objects[dsid].close()
    @update_manager.stop()

  getAllCachedUndeletedDatastoreIDs: ->
    return (dsid for dsid, datastore of @_cached_objects when not datastore.is_deleted())

  getCachedDatastore: (dsid) ->
    return @_cached_objects[dsid]

  _evict: (handle) ->
    dsid = @_handle_to_dsid_map[handle]
    return unless dsid?
    delete @_handle_to_dsid_map[handle]
    if dsid of @_cached_objects
      obj = @_cached_objects[dsid]
      obj.mark_deleted()
      if obj.is_closed()
        delete @_cached_objects[dsid]
    @update_manager.remove_poll handle

  close: (dsid) ->
    if dsid of @_cached_objects
      handle = @_cached_objects[dsid].get_handle()
      # don't delete from _handle_to_dsid_map yet; TODO: stop polling
      # when safe (when all deltas submitted)
      obj = @_cached_objects[dsid]
      obj.close()
      if obj.is_deleted()
        delete @_cached_objects[dsid]
    else
      throw new Error "Attempt to close unknown datastore: #{dsid}"
    # TODO(dropbox): stop polling when safe (when all deltas submitted)
    #@update_manager.remove_poll handle

  _handle_server_update: (data, cb) ->
    # Each "event" here is either a delta or a dslist, not both.  A
    # server response that contains both is split into two of these
    # "events" elsewhere.  Delta events without a delta indicate
    # datastore deletions and role changes.
    if data.dslist
      #console.log "received dslist update", data
      if @_dslist_listener_server
        @_dslist_listener_server data.dslist
      return cb null
    else
      #console.log "received delta", data
      handle = data.handle
      dsid = @_handle_to_dsid_map[handle]
      if not dsid?
        console.log "unknown handle #{handle} (maybe datastore was evicted)", data, @_handle_to_dsid_map, @_cached_objects
        return cb null
      if not data.delta?
        if data.notfound?
          @_evict handle
        else if data.role?
          if @_cached_objects[dsid]?
            @_cached_objects[dsid].role = data.role
        return cb null

      flob_delta = data.delta

      @_retrieve dsid, handle, (err, obj) =>
        return cb err if err
        obj.receive_server_delta flob_delta
        return cb null

  # calls cb err, ManagedDatastore
  open: (dsid, handle, cb) ->
    if @_cached_objects[dsid]
      @_cached_objects[dsid].open()
    @_retrieve dsid, handle, cb

  _retrieve: (dsid, handle, cb) ->
    cached = @_cached_objects[dsid]
    return cb null, cached if cached?

    @_handle_to_dsid_map[handle] = dsid

    # don't have it locally yet
    @flob_client.get_snapshot handle, (err, resp) =>
      return cb err if err?

      if @_cached_objects[dsid]?
        # if another call already retrieved
        # TODO(dropbox): Reason through whether we're safe when another
        # device deletes a cached private datastore and locally we
        # reopen it.  This will assign a different handle but it may
        # map to the same dsid and hence to the same cached object.
        return cb null, @_cached_objects[dsid]

      #log.debug "retrieved flob", flob

      ds = DatastoreModel.from_get_snapshot_resp resp
      resolver = new DefaultResolver
      obj = ManagedDatastore.fresh_managed_datastore dsid, handle, resp.role, ds, resp.rev, resolver, @flob_client, @_dslist_listener_local
      #log.debug "retrieved from server"

      # this is a new object, so we need to receive deltas on it
      @update_manager.add_poll handle, obj.sync_state.get_server_rev()
      @_cached_objects[dsid] = obj
      return cb null, obj


# TODO(dropbox): Add a comment with a state diagram.
