# Stores key-value pairs.
#
# Records belong to datastores, and are grouped into tables. Changes to records
# in the same datastore can be synchronized atomically.
#
# Record instances can be obtained by calling the methods defined in
# Dropbox.Datastore.Table. Record objects should not be constructed directly.
#
# Values of record fields are owned by the record and should not be
# mutated directly, except for Lists, which can be mutated with the
# List methods.
#
# @see Dropbox.Datastore.Table
# @see Dropbox.Datastore
class Dropbox.Datastore.Record
  # The maximum size in bytes of a record.
  #
  # @property {Number}
  @RECORD_SIZE_LIMIT: 100 * 1024

  # The size in bytes of a record before accounting for the sizes of its fields.
  #
  # The overall size of a record is this value plus the sum of the sizes of its fields.
  #
  # @property {Number}
  @BASE_RECORD_SIZE: 100

  # The size in bytes of a field before accounting for the sizes of its values.
  #
  # The overall size of a field is this value plus:
  #
  # - For ```String``` and ```Uint8Array```: the length in bytes of the value.
  # - For ```Dropbox.Datastore.List```: the sum of the size of each list item, where each item's
  #   size is computed as the size of the item value plus {Dropbox.Datastore.List}.BASE_ITEM_SIZE.
  # - For other atomic types: no additional contribution to the size of the field.
  @BASE_FIELD_SIZE: 100

  # Returns a field's value, or null if the field does not exist.
  #
  # The returned object will have one of the following types:
  #
  # * ```String```
  # * ```Number```
  # * ```Boolean```
  # * ```Date```
  # * ```Uint8Array```
  # * ```Dropbox.Datastore.List```
  #
  # The returned value can also be ```null```, which indicates that
  # the specified field does not exist on this record.
  #
  # Note that if the field holds a 64-bit integer, the return value
  # will be a boxed ```Number``` that approximates the 64-bit integer
  # as closely as possible, with an additional property ```dbxInt64```
  # that holds the precise signed integer value in decimal
  # representation as a string (see {Dropbox.Datastore.int64}). For
  # integers that are at most 2^53 in magnitude, the approximation is
  # exact.
  #
  # @param {String} fieldName the name of the field whose value will be
  #   returned
  # @return {String|Number|Boolean|Date|Uint8Array|Dropbox.Datastore.List|null}
  #   the value associated with the field
  get: (fieldName) ->
    @_checkNotDeleted()
    fields = @_rawFieldValues()
    return null unless fieldName of fields
    impl.fromDsValue @_datastore, @, fieldName, fields[fieldName]

  # Changes a field's value.
  #
  # A field can store a single value. This method discards the field's old
  # value. Future {Dropbox.Datastore.Record#get} calls will return the new value.
  #
  # The value can be one of several native JavaScript types:
  #
  # * ```String```
  # * ```Number```
  # * ```Boolean```
  # * ```Date```
  # * ```Uint8Array```
  # * ```Array```
  #
  # Setting a value of ```null``` deletes the field from the record. Special
  # considerations apply to certain types:
  #
  # * ```Number```: By default, a regular JavaScript number will be
  #   stored as a double for other platforms. To store the value
  #   instead as a 64-bit integer, use {Dropbox.Datastore.int64} to
  #   create a boxed number.
  # * ```Array```: Each element of the array
  #   can be any of the supported types except ```Array```. Elements
  #   of the array need not be of the same type.  When this field
  #   value is retrieved using {Dropbox.Datastore.Record#get}, it will
  #   be returned as a {Dropbox.Datastore.List}.
  #
  # @param {String} fieldName the name of the field whose value will be
  #   modified
  # @param {String|Number|Boolean|Date|Uint8Array|Array|null} value the
  #   value to store in the field
  # @return {Dropbox.Datastore.Record} this, for easy call chaining
  set: (fieldName, value) ->
    # @update calls @_checkNotDeleted
    arg = {}
    # update calls toDsValue
    arg[fieldName] = value
    @update arg

  # Returns a {Dropbox.Datastore.List} from the given field of the
  # record, creating an empty one if the field does not currently
  # exist on this record. An error is thrown if the field exists but
  # contains a value that is not a {Dropbox.Datastore.List}.
  #
  # Using this method is usually preferable to calling
  # {Dropbox.Datastore.Record#set} with an empty list: in cases where
  # multiple clients create a list on the same field at the same time,
  # {Dropbox.Datastore.Record#getOrCreateList} allows their changes to
  # be merged while {Dropbox.Datastore.Record#set} does not.
  #
  # @param {String} fieldName the name of the field containing the
  #   {Dropbox.Datastore.List}
  # @return {Dropbox.Datastore.List}
  #   the existing {Dropbox.Datastore.List} in the given field, or
  #   a newly created empty {Dropbox.Datastore.List} if the field
  #   did not previously exist
  getOrCreateList: (fieldName) ->
    @_checkNotDeleted()
    fields = @_rawFieldValues()

    if not fields[fieldName]?
      data = {}
      data[fieldName] = ['LC']
      @_storeUpdate data
      fields = @_rawFieldValues() # re-query to reflect the update
    else if not T.is_array fields[fieldName]
      throw new Error "Can't call getOrCreateList on field #{fieldName} for record (#{@tid}, #{@rid}): existing value #{fields[fieldName]} is not a list"

    return impl.fromDsValue @_datastore, @, fieldName, fields[fieldName]

  # Returns the values of all the fields in this record.
  #
  # A new object is created every time this method is called.
  # Performance-conscious code should not assume that this method caches its
  # return value.
  #
  # See {Dropbox.Datastore.Record#get} for a more in-depth discussion of
  # possible return values.
  #
  # @return {Object<String, String|Number|Boolean|Date|Uint8Array|Dropbox.Datastore.List>} a
  #   JavaScript object whose properties are the names of this record's fields,
  #   and whose values are the corresponding field values
  # @throw {Error} if this record was deleted from the datastore
  getFields: ->
    @_checkNotDeleted()
    fieldValues = {}
    for name, value of @_rawFieldValues()
      fieldValues[name] = impl.fromDsValue @_datastore, @, name, value
    fieldValues

  # Returns the size in bytes of this record.
  #
  # The overall size of a record is calculated by summing the size of all
  # values in all fields, plus the base size of an empty record itself.
  # A deleted record has a size of zero.
  #
  # @return {Number}
  getSize: ->
    @_managed_datastore.get_record_size(@_tid, @_rid)

  # Updates this record with the given field name/value pairs.
  #
  # A field can store a single value. This method discards the old values of
  # the modified fields. Future {Dropbox.Datastore.Record#get} calls will return
  # the new values.
  #
  # This method is conceptually equivalent to calling
  # {Dropbox.Datastore.Record#set} for each field. For a detailed
  # discussion on possible field values, see {Dropbox.Datastore.Record#set}.
  #
  # @param {Object<String, String|Number|Boolean|Date|Uint8Array|Array|null>} a
  #   JavaScript object whose properties are the names of the fields whose
  #   values will be changed, and whose values are the values that will be
  #   assigned to the fields
  # @return {Dropbox.Datastore.Record} this, for easy call chaining
  update: (fieldValues) ->
    @_datastore._checkWritable()
    @_checkNotDeleted()
    data = {}
    for name, value of fieldValues
      if value?
        data[name] = ['P', impl.toDsValue(value)]
      # TODO(dropbox): make this unconditional once the server allows redundant deletions
      else if (@get name)?
        data[name] = ['D']
    if not T.is_empty data
      @_storeUpdate data
    @

  # Deletes this record from the datastore.
  #
  # Once a record is deleted, the Record methods that operate on fields will
  # throw exceptions.
  #
  # @return {Dropbox.Datastore.Record} this, for easy call chaining
  deleteRecord: ->
    # NOTE: The natural name for this method would be "delete", but would
    # conflict with the "delete" JavaScript keyword.
    @_datastore._checkWritable()
    @_checkNotDeleted()
    @_deleted = true
    @_record_cache.remove @_tid, @_rid
    change = Change.from_array ['D', @_tid, @_rid]
    @_managed_datastore.perform_local_change change
    @_datastore._recordsChangedLocally [@]
    @

  # Returns true if this record has a value stored in a field.
  #
  # @param {String} fieldName the name of the field whose value will be
  #   looked up
  # @return {Boolean} true if this record has a value stored in the given
  #   field, false otherwise
  has: (fieldName) ->
    @_checkNotDeleted()
    fields = @_rawFieldValues()
    fieldName of fields

  # Returns this record's ID.
  #
  # A record's ID is established when the record is created, and cannot be
  # changed afterwards. Record IDs are unique in a table.
  #
  # {Dropbox.Datastore.Table#get} can be used to look up records by ID.
  #
  # @return {String} this record's ID; record IDs are unique in a table
  getId: () ->
    @_rid

  # Returns the table that this record belongs to.
  #
  # The association between a record and its table is established when the
  # record is created. Records cannot be moved between tables.
  #
  # @return {Dropbox.Datastore.Table} the table that this record belongs to
  getTable: () ->
    return @_datastore.getTable @_tid

  # Returns true if this record was deleted from the datastore
  #
  # @return {Boolean} true if this record was deleted from the datastore
  isDeleted: ->
    @_deleted

  # @private (not really private, but documenting this is not worth the space it takes)
  toString: ->
    fields = if @isDeleted() then 'deleted' else JSON.stringify @getFields()
    "Datastore.Record((#{@_tid}, #{@_rid}): #{fields})"

  # @private
  _rawFieldValues: ->
    @_managed_datastore.query @_tid, @_rid

  # @private
  _storeUpdate: (recordUpdate) ->
    change = Change.from_array ['U', @_tid, @_rid, recordUpdate]
    @_managed_datastore.perform_local_change change
    @_datastore._recordsChangedLocally [@]
    return

  # Checks that a string meets the constraints for record IDs.
  #
  # Record IDs must be strings containing 1-64 characters from the set
  # a-z, A-Z, 0-9, dot (.), underscore (_), plus sign (+), minus sign
  # (-), equal sign (=), forward slash (/).  The first character may
  # also be a colon (:), but such record IDs are reserved.
  #
  # This character set accomodates base64-encoded strings, as well as URL-safe
  # base64-encoded strings.
  #
  # @param {String} recordId the string to be checked
  # @return {Boolean} true if recordId can be used as a record ID, false
  #   otherwise
  @isValidId: (recordId) ->
    recordIdRe = new RegExp T.SS_ID_REGEX
    T.is_string(recordId) and recordIdRe.test(recordId)

  # @private
  constructor: (@_datastore, @_tid, @_rid) ->
    @_deleted = false
    @_record_cache = @_datastore._record_cache
    @_managed_datastore = @_datastore._managed_datastore

  # Makes sure that this instance does not represent a deleted record.
  #
  # @private
  # @throw {Error} if the record represented by this instance was deleted
  _checkNotDeleted: ->
    if @_deleted
      throw new Error "Attempt to operate on deleted record (#{@_tid}, #{@_rid})"
    return
