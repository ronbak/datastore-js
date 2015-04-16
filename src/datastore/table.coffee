# Stores structured data and organizes it for retrieval.
#
# Tables are designed to store similar records in a manner that supports
# retrieving subsets of the records that meet certain conditions.
#
# Datastore tables do not have the constraints associated with tables in SQL
# databases. A datastore table does not place any constraints on the fields in
# its records.
#
# Tables are created and deleted on-demand as records get added and
# removed. Table instances just keep track of the records associated
# with a table.
#
# Table instances can be obtained by calling {Dropbox.Datastore#getTable}.
# Table objects should not be constructed directly.
#
# @see Dropbox.Datastore
# @see Dropbox.Datastore#getTable
# @see Dropbox.Datastore.Record
class Dropbox.Datastore.Table
  # Returns the table's ID.
  #
  # @return {String} the table's ID; table IDs are unique across a datastore
  getId: ->
    return @_tid

  # Retrieves the record with the given ID.
  #
  # @param {String} recordId the ID of the record to be returned
  # @return {?Dropbox.Datastore.Record} the record in this table with the given
  #   ID; null if no record with the given ID exists
  # @throw {Error} if recordId is not a valid record Id
  # @see Dropbox.Datastore.Record.isValidId
  get: (recordId) ->
    unless Dropbox.Datastore.Record.isValidId recordId
      throw new Error("Invalid record ID: #{recordId}")

    record = @_record_cache.get @_tid, recordId
    if record?
      assert not record._deleted
      return record
    contents = @_managed_datastore.query @_tid, recordId
    if not contents?
      return null
    return @_record_cache.getOrCreate @_tid, recordId

  # Retrieves the record with the given ID, creating a new record if
  # there is no existing record with that ID.
  #
  # If a new record is created, it is populated with the specified
  # default values. The ```defaultValues``` parameter takes the same
  # form as the ```fieldValues``` parameter in
  # {Dropbox.Datastore.Record#update}.
  #
  # @param {String} recordId the ID of the record to be returned
  # @param {Object<String, String|Number|Boolean|Date|Uint8Array|Array|null>}
  #   defaultValues if a new record is created on-demand,
  #   this parameter will be used to populate the record's fields
  #
  # @return {Dropbox.Datastore.Record} the record in this table with the given
  #   ID; if no record with the given ID exists, a new record is created, with
  #   the fields specified by the defaultFields parameter
  # @throw {Error} if recordId is not a valid record ID
  # @see Dropbox.Datastore.Record.isValidId
  getOrInsert: (recordId, defaultValues) ->
    @_datastore._checkWritable()
    existing = @get recordId
    return existing if existing
    @_insertWithId recordId, defaultValues

  # Creates a new record in this table, populated with the given field
  # values.
  #
  # The ```fieldValues``` parameter takes the same form as the
  # ```fieldValues``` parameter in {Dropbox.Datastore.Record#update}.
  # A unique record ID is assigned automatically.
  #
  # @param {Object} fieldValues maps field names to the values that the fields
  #   should have in the newly created record
  # @return {Dropbox.Datastore.Record} the newly created record
  insert: (fieldValues) ->
    @_datastore._checkWritable()
    recordId = @_datastore._generateRid()
    assert not (@get recordId)?
    @_insertWithId recordId, fieldValues

  # Retrieves the records from this table whose fields match some values.
  #
  # @param {Object} fieldValues maps field names to values; only
  #   records where each field matches the given value will be
  #   returned.  This is equivalent to the AND operator in SQL
  #   queries.
  # @return {Array<Dropbox.Datastore.Record>} all the records in the table
  #   whose field values match the values in the fieldValues parameter
  query: (fieldValues) ->
    rids = @_managed_datastore.list_rows_for_table @_tid

    ret = []
    for rid in rids
      data = @_managed_datastore.query @_tid, rid
      if not fieldValues? or impl.matchDsValues fieldValues, data
        record = @get rid
        assert record?
        ret.push record
    ret

  # Sets a resolution rule for conflicts involving the given field,
  # which will be used when automatically merging local and remote
  # changes. The rule applies to all records in this table, and any
  # previously-set rule for the same field of the same table is
  # replaced. The "remote" rule is used by default if no rule is set.
  #
  # The valid rules are:
  #
  # * "remote": Resolves conflicts by selecting the remote change from the Dropbox server. This is the default conflict resolution rule.
  # * "local": Resolves conflicts by selecting the local change on this client.
  # * "max": Resolves conflicts by selecting the largest value, based on type-specific ordering.
  # * "min": Resolves conflicts by selecting the smallest value, based on type-specific ordering.
  # * "sum": Resolves conflicts by calculating a value such that all additions to or subtractions from a numerical value are preserved and combined. This allows a numerical value to act as a counter or accumulator without losing any updates. For non-numerical values this resolution rule behaves as "remote".
  #
  # Rules are not persistent, so you should always set up any
  # non-default resolution rules before making any changes to your
  # datastore.
  #
  # @param {String} fieldName the field that the rule applies to
  # @param {String} rule one of "remote", "local", "min", "max", or "sum"
  # @return {Dropbox.Datastore.Table} this, for easy call chaining
  setResolutionRule: (fieldName, rule) ->
    if rule not in ['remote', 'local', 'min', 'max', 'sum']
      throw new Error "#{rule} is not a valid resolution rule. Valid rules are 'remote', 'local', 'min', 'max', and 'sum'."
    @_managed_datastore.resolver.add_resolution_rule @_tid, fieldName, rule
    @

  # Checks that a string meets the constraints for table IDs.
  #
  # Table IDs must be strings containing 1-64 characters from the set
  # a-z, A-Z, 0-9, dot (.), underscore (_), plus sign (+), minus sign
  # (-), equal sign (=), forward slash (/).  The first character may
  # also be a colon (:), but such table IDs are reserved.
  #
  # This character set accomodates base64-encoded strings, as well as URL-safe
  # base64-encoded strings.
  #
  # @param {String} tableId the string to be checked
  # @return {Boolean} true if tableId can be used as a table ID, false
  #   otherwise
  @isValidId: (tableId) ->
    tableIdRe = new RegExp T.SS_ID_REGEX
    T.is_string(tableId) and tableIdRe.test(tableId)

  # @private
  # Use Dropbox.Datastore#getTable instead of calling this directly.
  constructor: (@_datastore, @_tid) ->
    @_record_cache = @_datastore._record_cache
    @_managed_datastore = @_datastore._managed_datastore

  # @private (not really private, but documenting this is not worth the space it takes)
  toString: ->
    "Datastore.Table(#{@_tid})"

  # @private
  _insertWithId: (rid, fieldVals) ->
    data = {}
    for k, v of fieldVals
      data[k] = impl.toDsValue v
    change = Change.from_array ['I', @_tid, rid, data]
    @_managed_datastore.perform_local_change change
    record = @_record_cache.getOrCreate @_tid, rid
    @_datastore._recordsChangedLocally [record]
    return record
