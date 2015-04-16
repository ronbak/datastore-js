# Stores and synchronizes structured data.
#
# Datastores store {Dropbox.Datastore.Record}s, which are key-value
# pairs. Records are grouped in {Dropbox.Datastore.Table}s, for
# efficient retrieval.
#
# Synchronization happens at the datastore scope, so only records that are in
# the same datastore can be changed together in an atomic fashion.
#
# Access control also happens at the datastore scope, so users can
# create datastores that can be shared with other users.  Datastores
# are shared by assigning roles (owner, editor, viewer, or none) to
# principals (public or team, the latter if the account is a Dropbox
# for business (DfB) account).  Any account with the correct permissions
# will be able to open the datastore by ID.
#
# Datastores are obtained from a {Dropbox.Datastore.DatastoreManager}.
class Dropbox.Datastore
  # The maximum size in bytes of a datastore.
  #
  # @property {Number}
  @DATASTORE_SIZE_LIMIT: 10 * 1024 * 1024

  # The maximum number of records in a datastore.
  #
  # @property {Number}
  @RECORD_COUNT_LIMIT: 100000

  # The size in bytes of a datastore before accounting for the size of its records.
  #
  # The overall size of a datastore is this value plus the size of all records.
  #
  # @property {Number}
  @BASE_DATASTORE_SIZE: 1000

  # This principal refers to the DfB team of the datastore's owner.
  # This is only valid if the owner is a member of a DfB team.
  #
  # @property {String}
  @TEAM: 'team'

  # This principal refers to the general public.
  #
  # @property {String}
  @PUBLIC: 'public'

  # This role indicates the owner of a datastore.
  #
  # @property {String}
  @OWNER: 'owner'

  # This role indicates edit permission of a datastore.
  #
  # @property {String}
  @EDITOR: 'editor'

  # This role indicates read-only viewing permission of a datastore.
  #
  # @property {String}
  @VIEWER: 'viewer'

  # This role indicates lack of access to a datastore.
  #
  # @property {String}
  @NONE: 'none'

  # Fires non-cancelable events every time a record changes, either
  # due to a local or remote change.
  #
  # Note: Since this is fired for local datastore changes, making
  # further changes in the listener can lead to infinite loops.  Use
  # {Dropbox.Datastore.RecordsChanged#isLocal} to determine if a
  # change was local or remote.
  #
  # @property {Dropbox.Util.EventSource<Dropbox.Datastore.RecordsChanged>}
  recordsChanged: null

  # Fires non-cancelable events every time the sync status changes
  #
  # @see Dropbox.Datastore#getSyncStatus
  #
  # @property {Dropbox.Util.EventSource<?>}
  syncStatusChanged: null

  # A Number instance that will be written to the datastore as a
  # 64-bit integer.
  #
  # Since JavaScript does not support 64-bit integers natively, an
  # ```int64``` is a boxed JavaScript number that approximates the
  # 64-bit integer as closely as possible, with an added property
  # ```dbxInt64``` that holds the precise signed integer value in
  # decimal representation as a string. For integers that are at most
  # 2^53 in magnitude, the approximation is exact.
  #
  # @param {String, Number} x an integer, a string holding a signed 64-bit
  #   integer in decimal representation, or the return value of a
  #   {Dropbox.Datastore.int64} call
  # @return {Number} an ```int64``` value
  # @throw {Error} if the argument cannot be interpreted as an ```int64```
  @int64: (x) ->
    if (T.is_number x) and x[impl.INT64_TAG]?
      return impl.validateInt64 x
    if T.is_string x
      if not impl.is_valid_int64_string x
        throw new Error "Not a valid int64 in string form: #{x}"
      y = new Number (parseInt x, 10)
      y[impl.INT64_TAG] = x
      return impl.validateInt64 y
    if (not T.is_number x) or not isFinite x
      throw new Error "Not a finite number: #{x}"
    if (Number x) != Math.round x
      throw new Error "Number is not an integer: #{x}"
    s = x.toFixed()
    if not impl.is_valid_int64_string s
      throw new Error "Number not in int64 range: #{x}"
    y = new Number x
    y[impl.INT64_TAG] = s
    return impl.validateInt64 y

  # Returns true if the argument is an int64 value as returned by
  # {Dropbox.Datastore.int64}.
  #
  # @return {Boolean}
  @isInt64: (x) ->
    return impl.isInt64 x

  # @private
  # Use {Dropbox.Datastore.DatastoreManager} instead of calling this directly.
  constructor: (@_datastore_manager, @_managed_datastore) ->
    @_dsid = @_managed_datastore.get_dsid()
    @_handle = @_managed_datastore.get_handle()
    @_record_cache = new RecordCache @
    @_last_used_timestamp = 0
    # FIXME: decide which events should be cancelable
    @recordsChanged = new Dropbox.Util.EventSource
    @syncStatusChanged = new Dropbox.Util.EventSource
    # This may be useful to instrument code for profiling, or perhaps
    # it could turn asynchronous uncaught errors into events that the
    # app can handle.
    #
    # TODO: think about exposing this
    @_timeoutWrapper = (f) -> f

    @_evt_mgr = new EventManager
    @_evt_mgr.register @_managed_datastore.syncStateChanged, (e) =>
      @_syncSoon()  # TODO(dropbox): only do this if there are new incoming changes
      @syncStatusChanged.dispatch null
    @_syncPending = false
    @_closed = false

    # initialize the table ourselves to circumvent the
    # isValidId check
    @_metadata_table = new Dropbox.Datastore.Table @, ':info'
    @_metadata_table.setResolutionRule 'mtime', 'max'

  # Gets the last time this datastore was modified.
  #
  # @return {Date} the last modified time
  getModifiedTime: ->
    metadata_record = @_metadata_table.get 'info'
    return null unless metadata_record?
    return metadata_record.get 'mtime'

  # Gets the title of this datastore. If the title was
  # never set, the returned value is ```null```.
  #
  # @return {String|null} the title of the datastore
  getTitle: ->
    metadata_record = @_metadata_table.get 'info'
    return null unless metadata_record?
    return metadata_record.get 'title'

  # Sets the title of this datastore
  #
  # @param {String|null} title the new title
  setTitle: (title) ->
    unless not title? or T.string title
      throw new Error "Title must be a string or null!"
    metadata_record = @_metadata_table.getOrInsert 'info', {}
    metadata_record.set 'title', title

  # Returns whether this datastore is shareable.  This is purely a
  # function of the datastore ID.  Only datastores created with
  # {Dropbox.Datastore.DatastoreManager#createDatastore} (i.e. whose
  # ID starts with ".") are shareable.
  #
  # @return {Boolean} True if this datastore is shareable, false otherwise
  isShareable: () ->
    return @_dsid[0] == '.'

  # @private
  _checkShareable: () ->
    unless @isShareable()
      throw new Error "Datastore is not shareable"

  # Gets the effective role of this datastore.  This indicates the
  # current user's access level.  The OWNER and EDITOR roles give full
  # control (reading, writing, changing roles); the VIEWER role gives
  # read-only control.  The OWNER role is established at datastore
  # creation and cannot be changed.  For non-shareable (private)
  # datastores OWNER is the only role.
  #
  # @return {String} the role of this datastore;
  #   <a href="#Dropbox.Datastore.OWNER">OWNER</a>,
  #   <a href="#Dropbox.Datastore.EDITOR">EDITOR</a>,
  #   <a href="#Dropbox.Datastore.VIEWER">VIEWER</a>,
  #   <a href="#Dropbox.Datastore.NONE">NONE</a>
  #   (but NONE should never occur; always OWNER for private
  #   datastores)
  getEffectiveRole: () ->
    return Dropbox.Datastore.OWNER unless @isShareable()
    role = @_managed_datastore.get_effective_role()
    return Dropbox.Datastore._roleFromInt role

  # Returns whether this datastore is writable.  This is a shorthand
  # for testing whether {Dropbox.Datastore#getEffectiveRole} returns
  # <a href="#Dropbox.Datastore.OWNER">OWNER</a> or
  # <a href="#Dropbox.Datastore.EDITOR">EDITOR</a>.
  #
  # @return {Boolean} True if this datastore is writable, false otherwise
  isWritable: () ->
    role = @getEffectiveRole()
    return role == Dropbox.Datastore.OWNER or role == Dropbox.Datastore.EDITOR

  # @private
  _checkWritable: () ->
    unless @isWritable()
      throw new Error "Datastore is not writable"

  # @private
  _checkRole: (role) ->
    unless role == Dropbox.Datastore.EDITOR or role == Dropbox.Datastore.VIEWER
      throw new Error "Invalid role: #{role}"

  # @private
  _checkPrincipal: (principal) ->
    unless principal == Dropbox.Datastore.TEAM or
           principal == Dropbox.Datastore.PUBLIC or
           principal.match(/^u[1-9][0-9]*$/)  # Undocumented u<UID>
      throw new Error "Invalid principal: #{principal}"

  # @private
  _getRole: (principal) ->
    irole = @getTable(impl.ACL_TID)?.get(principal)?.get('role')
    return Dropbox.Datastore.NONE unless irole?
    return Dropbox.Datastore._roleFromInt irole

  # Gets the role for a principal, for a shareable datastore.
  #
  # @param {String} principal
  #   <a href="#Dropbox.Datastore.TEAM">TEAM</a> or
  #   <a href="#Dropbox.Datastore.PUBLIC">PUBLIC</a>
  # @return {String} The role:
  #   <a href="#Dropbox.Datastore.OWNER">OWNER</a>,
  #   <a href="#Dropbox.Datastore.EDITOR">EDITOR</a>,
  #   <a href="#Dropbox.Datastore.VIEWER">VIEWER</a>, or
  #   <a href="#Dropbox.Datastore.NONE">NONE</a>
  getRole: (principal) ->
    @_checkShareable()
    @_checkPrincipal principal
    return @_getRole principal

  # Sets the role for a principal, for a shareable datastore.
  #
  # @param {String} principal
  #   <a href="#Dropbox.Datastore.TEAM">TEAM</a> or
  #   <a href="#Dropbox.Datastore.PUBLIC">PUBLIC</a>
  # @param {String} role
  #   <a href="#Dropbox.Datastore.EDITOR">EDITOR</a>,
  #   <a href="#Dropbox.Datastore.VIEWER">VIEWER</a>, or
  #   <a href="#Dropbox.Datastore.NONE">NONE</a>
  setRole: (principal, role) ->
    if role == Dropbox.Datastore.NONE
      @deleteRole principal
      return
    @_checkShareable()
    @_checkPrincipal principal
    @_checkRole role
    @_checkWritable()
    irole = Dropbox.Datastore.int64(Dropbox.Datastore._intFromRole(role))
    @getTable(impl.ACL_TID).getOrInsert(principal).update role: irole

  # Deletes the role for a principal, for a shareable datastore.
  #
  # @param {String} principal
  #   <a href="#Dropbox.Datastore.TEAM">TEAM</a> or
  #   <a href="#Dropbox.Datastore.PUBLIC">PUBLIC</a>
  deleteRole: (principal) ->
    @_checkShareable()
    @_checkPrincipal(principal)
    @_checkWritable()
    @getTable(impl.ACL_TID).get(principal)?.deleteRecord()

  # Lists the roles for all principals, for a shareable datastore.
  #
  # @return {Object} a mapping from principals
  #   (i.e. <a href="#Dropbox.Datastore.TEAM">TEAM</a> or <a
  #   href="#Dropbox.Datastore.PUBLIC">PUBLIC</a>) to roles (<a
  #   href="#Dropbox.Datastore.EDITOR">EDITOR</a> or <a
  #   href="#Dropbox.Datastore.VIEW">VIEW</a>).
  listRoles: () ->
    @_checkShareable()
    role_map = {}
    for record in @getTable(impl.ACL_TID).query()
      principal = record.getId()
      role_map[principal] = @_getRole(principal)
    return role_map    

  # @private
  @_roleFromInt: (irole) ->
    switch
      when irole >= impl.ROLE_OWNER then Dropbox.Datastore.OWNER
      when irole >= impl.ROLE_EDITOR then Dropbox.Datastore.EDITOR
      when irole >= impl.ROLE_VIEWER then Dropbox.Datastore.VIEWER
      else Dropbox.Datastore.NONE

  # @private
  @_intFromRole: (role) ->
    switch role
      when Dropbox.Datastore.OWNER then impl.ROLE_OWNER
      when Dropbox.Datastore.EDITOR then impl.ROLE_EDITOR
      when Dropbox.Datastore.VIEWER then impl.ROLE_VIEWER
      else 0

  # Creates a Table instance for a given table ID.
  #
  # @param {String} tableId the table's ID
  # @return {Dropbox.Datastore.Table} a Table object that can be used
  #   to insert or access records in the table with the given ID.  If
  #   this is a new table ID, the table will not be visible in
  #   {Dropbox.Datastore#listTableIds} until a record is inserted.
  # @throw {Error} if tableId is not a valid table ID
  # @see Dropbox.Datastore.Table.isValidId
  getTable: (tableId) ->
    @_checkNotClosed()
    unless Dropbox.Datastore.Table.isValidId(tableId)
      throw new Error("Invalid table ID: #{tableId}")
    new Dropbox.Datastore.Table @, tableId

  # The IDs of all the tables in this datastore.
  #
  # Tables with reserved names or containing no records are not listed.
  #
  # @return {Array<String>} the IDs of the tables in this datastore
  listTableIds: ->
    @_checkNotClosed()
    return @_managed_datastore.list_tables()

  # Returns the number of records in this datastore.
  #
  # @return {Number}
  getRecordCount: ->
    return @_managed_datastore.get_record_count()

  # Returns the size in bytes of this datastore.
  #
  # The overall size of a datastore is calculated by summing the size of all
  # records, plus the base size of an empty datastore itself.
  #
  # @return {Number}
  getSize: ->
    return @_managed_datastore.get_size()

  # @private (not really private, but documenting this is not worth the space it takes)
  toString: () ->
    closed = if @_closed then "[closed] " else ""
    return "Datastore(#{closed}#{@_dsid} [#{@_handle}])"

  # Closes the datastore.
  #
  # After a call to {Dropbox.Datastore#close}, you can no longer call methods
  # that read or modify tables or records of the datastore.
  #
  # The datastore will stop syncing once all outgoing changes have
  # been received by the server.
  #
  # @return {void}
  close: () ->
    @_closed = true
    @_evt_mgr.unregister_all()
    # TODO: temporary hack to remove all listeners
    @_listeners = []
    @_datastore_manager._obj_manager.close @_dsid
    undefined

  # Returns this datastore's ID.
  #
  # @return {String}
  getId: ->
    @_dsid

  # Returns an object representing the sync status of the datastore.
  #
  # The returned object has a single property:
  #
  # * uploading: ```true``` if there are changes to the datastore that
  #   have not been synced to the server yet.  This state should be
  #   transient unless, for example, the application is temporarily
  #   offline.  ```false``` otherwise.
  #
  # @return {Object} a plain object with an "uploading" property as
  #   described above
  getSyncStatus: ->
    # These are offered by the APIs for other languages but don't make
    # sense with auto-syncing (and as long as we don't have rollback):
    #
    # outgoing: True if there have been any database-mutating
    # operations (e.g. insert() or update()) since the last sync()
    # call (or since the database was opened, if sync() was never
    # called).
    #
    # incoming: True if there is a delta in the input queue, ready for
    # sync() to integrate into the datastore.
    #
    # downloading: True if the client library is currently in the
    # process of downloading one or more deltas from the server (but
    # these deltas haven't been fully received or added to the input
    # queue). Also true if we know that we should download a delta,
    # but we're offline.
    return {
      # NOTE: when enabling `outgoing' here, make sure to fire an event when it changes
      #outgoing: @_managed_datastore.has_unfinalized_changes()
      #incoming: @_managed_datastore.get_incoming_delta_count() > 0
      uploading: @_managed_datastore.get_outgoing_delta_count() > 0
      # This is an under-approximation, which should be OK.
      #downloading: false
    }

  # Checks that a string meets the constraints for datastore IDs.
  #
  # Valid IDs come in two forms.  The first form, used for private
  # datastores, has 1-64 characters from the set a-z, 0-9, dot (.),
  # minus sign (-), underscore (_), and may not begin or end with dot.
  #
  # The second form, used for shareable datastores, begins with a dot
  # (.), followed by 1-63 characters from the set a-z, A-Z, 0-9, minus
  # sign (-), underscore (_).
  #
  # @param {String} datastoreId the string to be checked
  # @return {Boolean} true if datastoreId can be used as a datastore ID, false
  #   otherwise
  @isValidId: (datastoreId) ->
    datastoreIdRe = new RegExp T.DS_ID_REGEX
    return T.is_string(datastoreId) and datastoreIdRe.test(datastoreId)

  # Check that a string represents a shareable datastore ID.
  #
  # This is a valid datastore ID starting with a '.'.
  #
  # @param {String} datastoreId the string to be checked
  # @return {Boolean} true if datastoreId can be used as a shareable
  #   datastore ID, false otherwise
  @isValidShareableId: (datastoreId) ->
    return @isValidId(datastoreId) and datastoreId[0] == '.'

  # @private
  _generateRid: () ->
    prefix = '_'
    infix = '_js_'  # to indicate javascript client
    now = Math.round (Date.now() * 1000)
    if now <= @_last_used_timestamp
      now = @_last_used_timestamp + 1
    @_last_used_timestamp = now
    encoded_timestamp = (now.toString 32)
    # 11 base-32 digits will be ok until Y3K.
    # (let ((digits 11) (base 32)) (format-time-string "%Y" (list 0 0 (expt base digits) 0)))
    while encoded_timestamp.length < 11
      encoded_timestamp = "0" + encoded_timestamp
    return prefix + encoded_timestamp + infix +
      # 1 billion possibilities: (expt 64 5)
      impl.randomWeb64String 5

  # @private
  _syncSoon: () ->
    if @_managed_datastore.is_deleted()
      # TODO(dropbox): think of a more informative error message;
      # this is likely to be triggered if the app tries to modify a
      # deleted datastore, triggering sync-state-changed, and finally
      # triggering _syncSoon
      throw new Error "Cannot sync deleted datastore #{@_dsid}"

    @_checkNotClosed()
    # Should we split "process outgoing changes soon" from "process
    # incoming changes soon"?
    if not @_syncPending
      @_syncPending = true
      setTimeout (@_timeoutWrapper =>
        @_syncPending = false
        @_sync()), 0
    undefined

  # @private
  #
  # Attempts to commit local changes to the server, first rebasing
  # them against any remote changes that have already been
  # downloaded. The rebase step will always succeed, but the changes
  # will not be accepted by the server if the server receives a change
  # from another client first.
  #
  # In order to guarantee that your changes get committed to the
  # server, you must listen on the syncStatusChanged event and keep
  # retrying until the number of outgoing deltas is zero.
  _sync: ->
    @_checkNotClosed()
    # sync has to be synchronous to avoid giving control back to the
    # app while the datastore is in a state where it can't serve reads
    # or accept changes.  If sync were an asynchronous method, the app
    # might call sync, and before sync finishes, might receive user
    # input that requires changes to the datastore while the datastore
    # is in an undefined state.
    #
    # So, sync will synchronously integrate incoming deltas into the
    # datastore, and put any pending changes into the outgoing queue
    # to be (asynchronously) submitted to the server.  When we add
    # persistence, sync may also trigger a write to IndexedDB, but
    # that would be asynchronous, and the notification that the
    # IndexedDB write succeeded would come through onSyncStatusChanged
    # or similar.  Or maybe we would maintain a synchronous
    # write-ahead log in local storage and apply to IndexedDB
    # asynchronously in a way that is transparent to the app.
    #
    # TODO(dropbox): if there are multiple tabs open, how is their
    # access to IndexedDB synchronized?  probably need to synchronize
    # through local storage, and combine the outgoing delta streams
    # into one by rebasing locally.
    remote_affected_record_map = @_managed_datastore.sync()
    recordsByTable = @_resolveAffectedRecordMap remote_affected_record_map
    found_something = false
    for tid, records of recordsByTable
      for record in records
        assert tid == record._tid, "tid mismatch"
        found_something = true
        rid = record._rid
        if not @_managed_datastore.query tid, rid
          record._deleted = true
          @_record_cache.remove tid, rid
    if found_something
      @recordsChanged.dispatch (new RecordsChanged recordsByTable, false)
    undefined

  # @private
  _resolveAffectedRecordMap: (m) ->
    recordsByTable = {}
    for tid, rids of m
      for rid of rids
        record = @_record_cache.getOrCreate tid, rid
        #console.log "affected record:", tid, rid, record
        if not recordsByTable[tid]?
          recordsByTable[tid] = []
        recordsByTable[tid].push record
    return recordsByTable

  # @private
  _recordsChangedLocally: (records) ->
    # We dispatch this synchronously rather than in a setTimeout 0,
    # since otherwise the UI may be out of sync with the data.  Not
    # sure it's a good idea to trigger this event right when the app
    # is in the middle of a (potentially compound) update of the
    # datastore, but let's see how it goes.
    if records.length > 0
      @recordsChanged.dispatch (RecordsChanged._fromRecordList records, true)
      @_syncSoon()
    undefined

  # @private
  _checkNotClosed: () ->
    if @_closed or not @_managed_datastore._open
      throw new Error "Datastore is already closed: #{@}"
    undefined
