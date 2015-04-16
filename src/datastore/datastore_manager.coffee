# @private
#
# EventSource that remembers the most recent event and calls new
# listeners with that event as soon as they are added.
#
# Also emits an event whenever the set of listeners changes.
class Dropbox.Datastore.impl.EventSourceWithInitialData extends Dropbox.Util.EventSource
  constructor: (@options) ->
    super options
    @_have_event = false
    @_last_event = null
    # This event is meant to enable DatastoreManager to start/stop
    # polling for the datastore list on first listener addition/last
    # listener removal, but DatastoreManager doesn't do that, yet.  So
    # it's unused.
    @_listenersChanged = new Dropbox.Util.EventSource

  _clearLastEvent: ->
    @_have_event = false
    @_last_event = null

  addListener: (listener) ->
    ret = super listener
    if @_have_event
      listener @_last_event
    @_listenersChanged.dispatch @_listeners
    return ret

  removeListener: (listener) ->
    ret = super listener
    # may be spurious
    @_listenersChanged.dispatch @_listeners
    return ret

  dispatch: (event) ->
    @_last_event = event
    @_have_event = true
    return super event


# @private
#
# This used to be in Dropbox.Datastore.DatastoreManager, but codo
# didn't seem to respect the @private there and made it a documented
# constant (despite it being inaccessible...).
DEFAULT_DATASTORE_ID = 'default'


# Lets you open, create, delete, and list datastores.
#
# Multiple instances of your app (running in different browser windows / tabs)
# can open the same datastore at the same time, and a single instance of your
# app can open multiple datastores at the same time.  But a single instance of
# your app can only have one {Dropbox.Datastore} instance for a each datastore
# at any time -- opening the same datastore twice is not allowed (unless you
# close it in between).
#
# See <a href="#Dropbox.Client.getDatastoreManager">Dropbox.Client.getDatastoreManager</a>
# to get an instance of DatastoreManager.
class Dropbox.Datastore.DatastoreManager

  # Fires non-cancelable events every time the datastore list
  # changes.  A listener that is added always receives at least one
  # call with the initial state.
  #
  # After creating or deleting a datastore, there may be a delay
  # before the corresponding event is fired.
  #
  # @property {Dropbox.Util.EventSource<Dropbox.Datastore.DatastoreListChanged>}
  datastoreListChanged: null

  # @private
  constructor: (client) ->
    unless client.isAuthenticated()
      throw new Error(
          "DatastoreManager requires an authenticated Dropbox.Client!")
    @datastoreListChanged = new Dropbox.Datastore.impl.EventSourceWithInitialData
    @_flob_client = new FlobClient client

    # Last datastore list received from server, without local overlay.
    # Initially null, but non-null after the first /await response,
    # and remains non-null from then on.
    @_lastListDsServerResponse = null

    # ObjectManager/FakeUpdateManager will immediately start polling
    # for the datastore list.
    @_obj_manager = new ObjectManager (new FakeUpdateManager @_flob_client), @_flob_client,
      ((data) => @_handleRemoteDslistUpdate data),
      (=> @_handleLocalDslistUpdate())

  # Shuts down the DatastoreManager.  All {Dropbox.Datastore}
  # instances obtained through this DatastoreManager become invalid.
  close: ->
    @_obj_manager.destroy()

  # @private (not really private, but documenting this is not worth the space it takes)
  toString: () ->
    "Datastore.DatastoreManager()"

  # @private
  _dispatchDslistEvent: ->
    server_resp = @_lastListDsServerResponse || new ListDatastoresResponse {datastores:[], token:"dummy"}
    @datastoreListChanged.dispatch new Dropbox.Datastore.DatastoreListChanged(
      @_getOverlaidDatastoreInfosFromListResponse server_resp)
    undefined

  # called when there is a local change to a datastore's metadata
  # @private
  _handleLocalDslistUpdate: ->
    @_dispatchDslistEvent()
    undefined

  # called when we get a list_datastores server response back
  # @private
  _handleRemoteDslistUpdate: (data) ->
    @_lastListDsServerResponse = data
    @_dispatchDslistEvent()
    undefined

  # @private
  #
  # dsResponse is null if that datastore wasn't in the listDatastores response from the server
  _getOverlaidDatastoreInfo: (dsid, dsResponse) ->
    cachedDatastore = @_obj_manager.getCachedDatastore dsid
    remoteInfo = (dsResponse?.info || {})
    if not cachedDatastore?
      infoFields = impl.clone remoteInfo
    else if not dsResponse? or dsResponse.rev < cachedDatastore.sync_state.get_server_rev()
      # no remote record, or local rev is higher -- use local record
      infoFields = cachedDatastore.datastore_model.getLocalInfoData()
    else
      # merge field-by-field
      infoFields = cachedDatastore.datastore_model.updateDatastoreInfo remoteInfo

    # convert ds values to js values
    for field, value of infoFields
      infoFields[field] =
        if T.is_array value
          # Avoid returning a Datastore.List object
          ((impl.fromDsValue null, null, null, v2) for v2 in value)
        else
          impl.fromDsValue null, null, null, value

    if T.is_empty infoFields
      # The distinction between {} (record present but empty) and null
      # (record absent) for datastore info is not something we
      # preserve elsewhere in the system, so we conflate the two here
      # as well.
      infoFields = null

    # TODO: compare handles to deal with deletion/recreation cases
    handle = if dsResponse?.handle? then dsResponse.handle else cachedDatastore.get_handle()

    # TODO: use cached role?
    role = dsResponse?.role ? impl.ROLE_OWNER

    return new Dropbox.Datastore.DatastoreInfo dsid, handle, infoFields, role

  # @private
  #
  # Overlays the server's response with unsynced local updates, and
  # converts it into a list of DatastoreInfo objects.
  _getOverlaidDatastoreInfosFromListResponse: (resp) ->
    ListDatastoresResponse.Type resp
    cachedIDs = @_obj_manager.getAllCachedUndeletedDatastoreIDs()
    # Maps all relevant datastore ids to the server's
    # ListDatastoresResponseItem, or null if they are missing from the
    # server's response.
    map = {}
    for dsid in cachedIDs
      map[dsid] = null
    for dsResponse in resp.datastores
      map[dsResponse.dsid] = dsResponse
    return (@_getOverlaidDatastoreInfo id, dsInfo for id, dsInfo of map)

  # @private
  _wrapDatastore: (managed_datastore, created) ->
    if created
      managed_datastore._update_mtime()
      managed_datastore.sync()
    return new Dropbox.Datastore @, managed_datastore

  # @private
  #
  # Calls callback err, datastore.
  _getOrCreateDatastoreByDsid: (dsid, callback) ->
    # TODO(dropbox): make this work offline
    @_flob_client.get_or_create_db dsid, (err, resp) =>
      return callback err if err?
      if not resp.handle?
        return callback new Error "get_or_create_datastore failed for #{dsid}"
      @_obj_manager.open dsid, resp.handle, (err, managed_datastore) =>
        return callback err if err?
        return callback null, (@_wrapDatastore managed_datastore, resp.created)
    undefined

  # @private
  #
  # Calls callback err, datastore.
  _createDatastore: (dsid, key, callback) ->
    # TODO(dropbox): make this work offline
    @_flob_client.create_db dsid, key, (err, resp) =>
      return callback err if err?
      if not resp.handle?
        return callback new Error "create_datastore failed for #{dsid}"
      @_obj_manager.open dsid, resp.handle, (err, managed_datastore) =>
        return callback err if err?
        return callback null, (@_wrapDatastore managed_datastore, resp.created)
    undefined

  # @private
  #
  # Calls callback err, datastore.
  _getExistingDatastoreByDsid: (dsid, callback) ->
    # TODO(dropbox): make this work offline
    @_flob_client.get_db dsid, (err, resp) =>
      return callback err if err?
      if not resp.handle?
        return callback new Error "Datastore #{dsid} not found or not accessible"
      @_obj_manager.open dsid, resp.handle, (err, managed_datastore) =>
        return callback err if err?
        return callback null, (new Dropbox.Datastore @, managed_datastore)
    undefined

  # The open* and create* methods are async so that not every getter
  # method on Datastore has to be async.  It makes sense to actually
  # do the RPC (or IndexedDB lookup) when the app says open, rather
  # than pretending that open is immediate, and then returning an
  # object that can't do anything until the I/O is complete.

  # TODO(pwnall): when opening, consider creating a new client off of the same credentials;
  #               currently, calling signOff means last-second changes won't
  #               sync
  # TODO(pwnall): when opening, exception if the client isn't authenticated

  # Asynchronously opens your app's default datastore for the current
  # user, then calls ```callback``` with the corresponding
  # ```Datastore``` object (or an error).
  #
  # @param {function(Dropbox.ApiError, Dropbox.Datastore)} callback
  #   called when the operation completes; if successful, the second parameter
  #   is the default datastore and the first parameter is null
  openDefaultDatastore: (callback) ->
    @_getOrCreateDatastoreByDsid DEFAULT_DATASTORE_ID, callback
    undefined

  # Asynchronously opens or creates the datastore with the given ID,
  # then calls ```callback``` with the corresponding ```Datastore```
  # object (or an error).
  #
  # @param {String} datastoreId the ID of the datastore to be opened
  # @param {function(Dropbox.ApiError, Dropbox.Datastore)} callback
  #   called when the operation completes; if successful, the second parameter
  #   is the default datastore and the first parameter is null
  # @see Dropbox.Datastore.isValidId
  openOrCreateDatastore: (datastoreId, callback) ->
    # TODO(dropbox): only allow local ids
    @_getOrCreateDatastoreByDsid datastoreId, callback
    undefined

  # Asynchronously opens the datastore with the given ID, then calls
  # ```callback``` with the corresponding ```Datastore``` object (or
  # an error).  The datastore must already exist.
  #
  # @param {String} datastoreId the ID of the datastore to be opened
  # @param {function(Dropbox.ApiError, Dropbox.Datastore)} callback
  #   called when the operation completes; if successful, the second parameter
  #   is the datastore and the first parameter is null
  # @see Dropbox.Datastore.isValidId
  openDatastore: (datastoreId, callback) ->
    @_getExistingDatastoreByDsid datastoreId, callback
    undefined

  # Asynchronously creates a new datastore, then calls ```callback```
  # with the corresponding ```Datastore``` object (or an error).
  #
  # @param {function(Dropbox.ApiError, Dropbox.Datastore)} callback
  #   called when the operation completes; if successful, the second parameter
  #   is the created datastore and the first parameter is null
  createDatastore: (callback) ->
    # "at least 256 bits"
    key = impl.randomWeb64String Math.ceil (256 / 6)
    dsid = ".#{impl.dbase64FromBase64 (Dropbox.Util.sha256 key)}"
    @_createDatastore dsid, key, callback
    undefined

  # Asynchronously deletes the datastore with the given ID, then calls
  # ```callback```.
  #
  # Deleting a nonexistent datastore is not considered an error.
  #
  # @param {String} datastoreId the ID of the datastore to be opened
  # @param {function(Dropbox.ApiError)} callback called when the
  #   operation completes; if successful, the parameter it is called
  #   with is null
  deleteDatastore: (dsid, callback) ->
    # TODO: make this work offline -- we probably have to buffer the
    # RPC and make sure a following call to openDefaultDatastore
    # returns a different datastore
    @_flob_client.get_db dsid, (err, resp) =>
      return callback err if err?
      if not resp.handle?
        return callback new Error "Datastore #{dsid} not found or not accessible"
      @_flob_client.delete_db resp.handle, (err) ->
        return callback err if err?
        # # TODO(dropbox): We should immediately evict this datastore
        # # instead of waiting for the deletion to be echoed back by the
        # # server in the await longpoll. To do this we need the real
        # # dsid and also make sure the eviction doesn't interfere with
        # # the dslist polling mechanism.
        # @_obj_manager.evict real_dsid
        callback null
    undefined

  # Asynchronously retrieves {Dropbox.Datastore.DatastoreInfo} objects
  # for all datastores accessible to your app as the current user,
  # then calls ```callback``` with the result (or an error).
  #
  # After creating, deleting, or modifying a datastore, there may be a
  # delay before the change is reflected in the list of datastores.
  #
  # @param {function(Dropbox.ApiError, Array<Dropbox.Datastore.DatastoreInfo>)}
  #   callback called when the operation completes; if successful, the second
  #   parameter is an Array of {Dropbox.Datastore.DatastoreInfo} and the first
  #   parameter is null
  listDatastores: (callback) ->
    if @_lastListDsServerResponse?
      return callback null, @_getOverlaidDatastoreInfosFromListResponse @_lastListDsServerResponse
    @_flob_client.list_dbs (err, resp) =>
      return callback err if err?
      # We don't set @_lastListDsServerResponse since that would be racy.
      callback null, @_getOverlaidDatastoreInfosFromListResponse resp
    undefined
