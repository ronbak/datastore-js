THE_DSID = '_mock_dsid_'
SECOND_DSID = '_second_mock_dsid_'
THE_HANDLE = '_mock_handle_'

impl = Dropbox.Datastore.impl
clone = impl.clone
T = impl.T

class MockServer
  @THE_DSID = THE_DSID
  @SECOND_DSID = SECOND_DSID
  @THE_HANDLE = THE_HANDLE

  constructor: ->
    @reset()

  reset: ->
    clearTimeout @_notify_timeout if @_notify_timeout?
    @_notify_timeout = null

    @delta_list = []
    @rev_caps = {}

    @_next_subid = 0
    @to_notify = {}
    # handlers for rev_cap being raised
    @client_notify_handlers = {}

    @_rev_handlers = {}
    @_list_datastores_entry = [
      dsid: THE_DSID
      handle: THE_HANDLE
      rev: @delta_list.length
      info: {}
    ]

  register_notify_handler: (cid, handler) ->
    @client_notify_handlers[cid] = handler

  cur_rev: ->
    return @delta_list.length

  # be notified when delta list gets this long
  subscribe_rev: (rev, handler) ->
    if rev <= @delta_list.length
      return handler()

    @_rev_handlers[rev] ?= []
    @_rev_handlers[rev].push handler

  setListDatastoresResponse: (entries) ->
    @_list_datastores_entry = entries

  list_datastores: (cb) ->
    cb null, new impl.ListDatastoresResponse {datastores: @_list_datastores_entry, token: "fake token"}

  delete_datastore: (handle, cb) ->
    T.assert handle == THE_HANDLE, -> "can only handle handle #{THE_HANDLE}, not #{handle}"
    @reset()
    return cb null

  get_datastore: (dsid, cb) ->
    T.assert dsid in [THE_DSID, SECOND_DSID], -> "can only handle dsid #{THE_DSID} or #{SECOND_DSID}, not #{dsid}"
    return cb null, {handle: THE_HANDLE, rev: @delta_list.length}

  get_snapshot: (handle, cb) ->
    T.assert handle == THE_HANDLE, -> "can only handle handle #{THE_HANDLE}, not #{handle}"
    T.assert @delta_list.length == 0, -> "can't get snapshot after we've already applied deltas"
    return cb null, {"rows": [], "rev": 0}

  put_delta: (handle, delta, cb) ->
    T.assert handle == THE_HANDLE, -> "can only handle handle #{THE_HANDLE}, not #{handle}"
    #console.log 'put delta', JSON.stringify delta

    rev = delta.rev
    assert rev <= @delta_list.length, "rev #{rev} is too high, current delta length is #{@delta_list.length}"

    if rev < @delta_list.length
      return cb null, {conflict:"conflict"}

    if rev == @delta_list.length
      # Turn it from a fancy struct into a plain JSON delta
      @delta_list.push JSON.parse JSON.stringify delta
      #console.log "delta_list is now", @delta_list
      @_future_maybe_notify()
    resp = {rev: @delta_list.length}
    return cb null, resp

  raise_cap: (cid, rev_cap) ->
    @rev_caps[cid] = rev_cap
    @_maybe_notify()

  _future_maybe_notify: ->
    return if @_notify_timeout?
    to_run = =>
      @_maybe_notify()
      @_notify_timeout = null
    @_notify_timeout = setTimeout to_run, 0

  _maybe_notify: ->
    package_deltas = (deltas) ->
      resp = {get_deltas: {deltas: {}}}
      resp.get_deltas.deltas[THE_HANDLE] = {deltas: deltas}
      return new impl.AwaitResponse resp

    sub_list = ([subid, sub_info] for subid, sub_info of @to_notify)
    for [subid, {cid, rev, handler}] in sub_list
      cap = @rev_caps[cid]
      num_new = Math.min @delta_list.length - rev, cap - rev

      if num_new > 0
        deltas = @delta_list.slice rev, (rev + num_new)
        delete @to_notify[subid]
        #console.log "notifying #{cid} of #{rev}:", deltas
        handler null, (package_deltas deltas)

  await: (cid, rev, cb) ->
    @to_notify[@_next_subid++] =
      cid: cid
      rev: rev
      handler: cb
    @rev_caps[cid] ?= rev
    @_maybe_notify()


class MockClient
  constructor: (@cid, @mock_server) ->
    @_put_delta_buffer = []

  # not in Dropbox.Client
  reset: ->
    @_put_delta_buffer = []

  # not in Dropbox.Client
  release_put_deltas: ->
    for [handle, delta, callback] in @_put_delta_buffer
      @mock_server.put_delta handle, delta, callback

  isAuthenticated: -> true

  _listDatastores: (callback) ->
    @mock_server.list_datastores (err, resp) ->
      return callback err if err
      return callback null, resp

  _getOrCreateDatastore: (path, callback) ->
    throw new Error "not supported"

  _getDatastore: (dsid, callback) ->
    @mock_server.get_datastore dsid, (err, resp) ->
      return callback err if err
      callback null, resp

  _deleteDatastore: (handle, callback) ->
    @mock_server.delete_datastore handle, callback

  _getDeltas: (handle, from_rev, callback) ->
    throw new Error "not supported"

  # put deltas won't happen unless called manually
  _putDelta: (handle, delta, callback) ->
    T.assert handle == THE_HANDLE, -> "can only handle handle #{THE_HANDLE}, not #{handle}"
    #console.log @cid, 'putting delta', delta
    @_put_delta_buffer.push [handle, delta, callback]

  _getSnapshot: (handle, callback) ->
    @mock_server.get_snapshot handle, (err, resp) =>
      return callback err if err
      return callback null, resp

  _datastoreAwait: (revisionMap, db_list_token, callback) ->
    # TODO: only handle delta awaits for now
    rev = revisionMap[THE_HANDLE]
    unless rev?
      # drop the request... it should be cancelled later
      return

    @mock_server.await @cid, rev, callback


exports.MockServer = MockServer
exports.MockClient = MockClient
