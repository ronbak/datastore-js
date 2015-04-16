USER_AGENT_PARAM = 'X-Dropbox-User-Agent'
REQUEST_ID_HEADER = 'X-Dropbox-Request-Id'

# @mixin
Dropbox.DatastoresClient =

  # @private
  #
  # default options: { isLongPoll: false, setRequestId: false }
  #
  # setRequestId defaults to false since that can save a round-trip.
  # We set it to true only for requests that are not
  # latency-sensitive.
  _dispatchDatastoreXhr: (method, url, params, responseType, options, callback) ->
    xhr = new Dropbox.Util.Xhr method, url
    if options.setRequestId
      # Custom headers like this one make all XHRs require a preflight
      # request, causing an extra round-trip every 5 minutes (when the
      # CORS cache expires) for every endpoint, which seems OK for
      # /await and /put_delta since they aren't _that_
      # latency-sensitive; but it's not OK for /list_datastores and
      # /get_snapshot since these can be on the critical path of the app
      # showing information to the user.
      reqId = 'xxxxxxxxxxxxxxxx'.replace /x/g, ->
        (Math.floor(Math.random()*16)).toString 16
      xhr.setHeader REQUEST_ID_HEADER, reqId

    # We can't set the "User-Agent" header, as specified in
    # http://www.w3.org/TR/XMLHttpRequest/#the-setrequestheader()-method
    # .  So we use a parameter instead.
    assert not params[USER_AGENT_PARAM]?
    params = impl.clone params
    params[USER_AGENT_PARAM] = "dropbox-js-datastore-sdk/#{DROPBOX_JS_VERSION}"

    xhr.setParams params

    # cacheFriendly means "use a header rather than a param for the
    # OAuth token", but with OAuth2, the token is a constant anyway,
    # so using a param doesn't seem any less cache-friendly.  So we
    # always set cacheFriendly to false, since that has the advantage
    # of helping avoid preflight requests.
    xhr.signWithOauth @_oauth, false

    cb = (error, data) ->
      return callback error if error?
      return callback null, responseType.fromJSON data
    if options.isLongPoll
      @_dispatchLongPollXhr xhr, cb
    else
      @_dispatchXhr xhr, cb

    xhr


  # The datastores that the application can access in the user's Dropbox.
  #
  # @private
  #
  # @param {function(?Dropbox.ApiError, ?Array<Dropbox.Datastore.Stat>}
  #   callback called with the result of the API request; if the call succeeds,
  #   the second parameter is an array of Dropbox.Datastore.Stat instances
  #   describing the datastores
  # @return {Dropbox.Xhr} the XHR object used for this API call
  _listDatastores: (callback) ->
    @_dispatchDatastoreXhr 'GET', @_urls.listDbs, {}, ListDatastoresResponse, {}, callback

  # @private
  # This is a low-level API used by Dropbox.Datastore. It should not be called
  # directly.
  #
  # @return {Dropbox.Xhr} the XHR object used for this API call
  _getOrCreateDatastore: (dsid, callback) ->
    @_dispatchDatastoreXhr 'POST', @_urls.getOrCreateDb, { dsid: dsid },
      CreateDatastoreResponse, {}, callback

  # @private
  #
  # This takes a key arg for idempotency; the key needs to be chosen
  # at the layer that implements retrying (or further out).  To avoid
  # recomputing the SHA-256 hash on every retry, we also take the
  # dsid (which is derived from the key) as an arg.
  _createDatastore: (dsid, key, callback) ->
    @_dispatchDatastoreXhr 'POST', @_urls.createDb, { dsid: dsid, key: key },
      CreateDatastoreResponse, {}, callback

  # @private
  _getDatastore: (dsid, callback) ->
    @_dispatchDatastoreXhr 'GET', @_urls.getDb, { dsid: dsid }, GetDatastoreResponse, {}, callback

  # Removes a datastore from a user's Dropbox.
  #
  # Removing a datastore is irreversible.
  #
  # @private
  # This is a low-level API used by Dropbox.Datastore. It should not be called
  # directly.
  #
  # @return {Dropbox.Xhr} the XHR object used for this API call
  _deleteDatastore: (handle, callback) ->
    @_dispatchDatastoreXhr 'POST', @_urls.deleteDb, { handle: handle },
      DeleteDatastoreResponse, { setRequestId: true }, callback

  # not needed for now
  ## Returns deltas that were applied after a datastore revision.
  ##
  ## @private
  ## This is a low-level API used by Dropbox.Datastore. It should not be called
  ## directly.
  ##
  ## @return {Dropbox.Xhr} the XHR object used for this API call
  #_getDeltas: (handle, from_rev, callback) ->
  #  ...

  # Applies a delta to the datastore in the user's Dropbox.
  #
  # @private
  # This is a low-level API used by Dropbox.Datastore. It should not be called
  # directly.
  #
  # @return {Dropbox.Xhr} the XHR object used for this API call
  _putDelta: (handle, delta, callback) ->
    @_dispatchDatastoreXhr 'POST', @_urls.putDelta, {
      handle: handle
      rev: delta.rev
      nonce: delta.nonce
      changes: JSON.stringify delta.changes
    }, PutDeltaResponse, { setRequestId: true }, callback

  # Fetches the data in a datastore at a recent revision.
  #
  # @private
  # This is a low-level API used by Dropbox.Datastore. It should not be called
  # directly.
  _getSnapshot: (handle, callback) ->
    @_dispatchDatastoreXhr 'GET', @_urls.getSnapshot, { handle: handle },
      GetSnapshotResponse, {}, callback

  # @private
  # This is a low-level API used by Dropbox.Datastore. It should not be called
  # directly.
  _datastoreAwait: (revisionMap, db_list_token, callback) ->
    @_dispatchDatastoreXhr 'POST', @_urls.datastoreAwait, {
      get_deltas: JSON.stringify
        cursors: revisionMap
      list_datastores: JSON.stringify
        token: db_list_token
    }, AwaitResponse, {
      isLongPoll: true
      setRequestId: true
    }, callback

  # Returns a DatastoreManager, which lets you access the user's
  # datastores in Dropbox.
  #
  # @return {Dropbox.Datastore.DatastoreManager} a datastore manager
  getDatastoreManager: () ->
    if not @_datastoreManager?
      @_datastoreManager = new Dropbox.Datastore.DatastoreManager @
      on_signout = =>
        if @authStep is Dropbox.Client.SIGNED_OUT
          @_datastoreManager.close()
          @_datastoreManager = null
          @onAuthStepChange.removeListener on_signout
      @onAuthStepChange.addListener on_signout

    @_datastoreManager

# mix into Dropbox.Client
do ->
  for method_name, method of Dropbox.DatastoresClient
    Dropbox.Client.prototype[method_name] = method
