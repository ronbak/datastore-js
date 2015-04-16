# A list of values stored in a field of a record.
#
# Instances of this class are returned by
# {Dropbox.Datastore.Record#getOrCreateList}, and by
# {Dropbox.Datastore.Record#get} after the field has been set to an
# array value.
#
# Each list element can be a ```String```, ```Boolean```, ```Number```
# (including ```Dropbox.Datastore.int64```), ```Date```, or
# ```Uint8Array```.  Lists can mix elements of different types.
#
# Modifications of the list will result in modifications of the
# datastore record field where the list is stored.
#
# List elements are owned by the list and should not be mutated.
#
# Whenever a method takes an index argument, negative numbers count
# from the end of the list.
class Dropbox.Datastore.List
  # The size in bytes of a list item.
  #
  # The overall size of a list item is this value plus the size of the item value.
  @BASE_ITEM_SIZE: 20

  # @private
  constructor: (@_datastore, @_record, @_field) ->

  # @private (not really private, but documenting this is not worth the space it takes)
  toString: ->
    return "Datastore.List((#{@_record._tid}, #{@_record._rid}, #{@_field}): #{JSON.stringify @_array})"

  # @private
  _array: ->
    # returns the underlying raw array
    # NB: it's possible that this isn't actually an array if it
    # changed out from under us
    return @_record._rawFieldValues()[@_field]

  # @private
  _checkValid: ->
    @_record._checkNotDeleted()
    unless T.is_array @_array()
      throw new Error "Attempt to operate on deleted list (#{@_record._tid}, #{@_record._rid}, #{@_field})"

  # @private
  _storeUpdate: (fieldUpdate) ->
    recordUpdate = {}
    recordUpdate[@_field] = fieldUpdate
    @_record._storeUpdate recordUpdate
    undefined

  # @private
  _fixInsertionIndex: (x) ->
    if not T.is_json_number x
      throw new RangeError "Index not a number: #{x}"
    len = @_array().length
    y = if x >= 0 then x else len + x
    if 0 <= y <= len
      return y
    throw new RangeError "Bad index for list of length #{len}: #{x}"

  # @private
  _fixIndex: (x) ->
    y = @_fixInsertionIndex x
    len = @_array().length
    if y < len
      return y
    throw new RangeError "Bad index for list of length #{len}: #{x}"

  ## We can't override the [] operator, which puts a pretty low limit
  ## on how close we can get to looking like a native array.  So we
  ## won't bother emulating .length as a field, and offer a method
  ## instead.

  # Returns the list element at ```index```.
  #
  # @param {Number} index
  # @return {String|Number|Boolean|Date|Uint8Array}
  get: (index) ->
    @_checkValid()
    dsValue = impl.clone @_array()[@_fixIndex index]
    return (impl.fromDsValue undefined, undefined, undefined, dsValue)

  # Sets the list element at ```index``` to ```value```.
  #
  # @param {Number} index
  # @param {String|Number|Boolean|Date|Uint8Array} value
  set: (index, value) ->
    @_checkValid()
    index = @_fixIndex index
    @_storeUpdate ['LP', index, (impl.toDsValue value, false)]
    undefined

  # Returns the number of elements in the list.
  #
  # @return {Number} the length of the list
  length: () ->
    @_checkValid()
    return @_array().length


  ## Mutators copied from Array

  # Like ```Array.pop```, removes the last element from the list and
  # returns it.
  #
  # @return {String|Number|Boolean|Date|Uint8Array} the element that
  #   was removed
  pop: ->
    @_checkValid()
    if @_array().length == 0
      throw new Error "List is empty"
    return @remove @_array.length - 1

  # Like ```Array.push```, appends the given value to the end of the list.
  #
  # @param {String|Number|Boolean|Date|Uint8Array} value
  push: (value) ->
    @_checkValid()
    @insert @_array().length, value
    undefined

  #reverse: ->

  # Like ```Array.shift```, removes the first element from the list
  # and returns it.
  #
  # @return {String|Number|Boolean|Date|Uint8Array} the element that
  #   was removed
  shift: ->
    @_checkValid()
    if @_array().length == 0
      throw new Error "List is empty"
    return @remove 0

  # Like ```Array.unshift```, inserts the given value at the beginning of the list.
  #
  # @param {String|Number|Boolean|Date|Uint8Array} value the value to insert
  unshift: (value) ->
    @insert 0, value
    undefined

  #sort: ->

  # Like ```Array.splice```, removes ```howMany``` consecutive
  # elements from the list starting at ```index```, then inserts
  # ```elements``` (if any) at ```index```.
  #
  # @param {Number} index
  # @param {Number} howMany the number of elements to remove
  # @return {Array<String|Number|Boolean|Date|Uint8Array>} the removed
  #   elements as an Array
  splice: (index, howMany, elements...) ->
    @_checkValid()
    if (not T.is_json_number howMany) or (howMany < 0)
      throw new RangeError "Bad second arg to splice: #{index}, #{howMany}"
    index = @_fixInsertionIndex index
    # slice will check more args
    result = @slice index, index + howMany
    for i in [0...howMany]
      @remove index
    for x in elements
      @insert index, x
      index++
    return result

  ## Custom mutators

  # Moves the element at ```oldIndex``` to position ```newIndex```.
  #
  # This is similar to removing an element from ```oldIndex```, then
  # inserting it at ```newIndex```, except that this is expressed as a
  # "move" operation for conflict resolution, which means it won't
  # result in duplicate elements like a removal and insertion would.
  #
  # After the move, the element formerly at ```oldIndex``` will be at
  # ```newIndex```.
  #
  # This method does nothing if ```oldIndex``` and ```newIndex``` are
  # the same.
  #
  # @param {Number} oldIndex
  # @param {Number} newIndex
  move: (oldIndex, newIndex) ->
    @_checkValid()
    oldIndex = @_fixIndex oldIndex
    newIndex = @_fixIndex newIndex
    if oldIndex == newIndex
      # no-op, but no point in throwing an error
      return undefined
    @_storeUpdate ['LM', oldIndex, newIndex]
    undefined

  # Removes the element at ```index``` and returns it.
  #
  # Elements with indexes greater than ```index``` are shifted to the
  # left.
  #
  # @param {Number} index
  # @return {String|Number|Boolean|Date|Uint8Array} the removed element
  remove: (index) ->
    @_checkValid()
    index = @_fixIndex index
    value = @get index
    @_storeUpdate ['LD', index]
    return value

  # Inserts the given ```value``` at ```index```.
  #
  # Elements with indexes greater than or equal to ```index``` are
  # shifted to the right.
  #
  # @param {Number} index
  # @param {String|Number|Boolean|Date|Uint8Array} value
  insert: (index, value) ->
    @_checkValid()
    index = @_fixInsertionIndex index
    @_storeUpdate ['LI', index, (impl.toDsValue value, false)]
    undefined


  ## Accessors copied from Array

  #concat: ->

  #join: ->

  # Returns an array that contains the elements of this list between
  # ```from``` (inclusive) and ```to``` (exclusive).
  #
  # The returned array is a copy, so the elements will not update as
  # changes are applied.
  #
  # @param {Number} from
  # @param {Number} to
  # @return {Array<String|Number|Boolean|Date|Uint8Array>}
  slice: (from, to) ->
    @_checkValid()
    return ((impl.fromDsValue undefined, undefined, undefined, x) for x in (@_array().slice from, to))

  # TODO(dropbox): implement
  #indexOf: ->

  # TODO(dropbox): implement
  #lastIndexOf: ->


  ## Custom accessors

  # Returns an array with the same elements as this list.
  #
  # The returned array is a copy, so the elements will not update as
  # changes are applied.
  #
  # @return {Array<String|Number|Boolean|Date|Uint8Array>}
  toArray: ->
    @_checkValid()
    return ((impl.fromDsValue undefined, undefined, undefined, x) for x in @_array().slice())


  ## Iteration methods copied from Array

  # TODO(dropbox): do we want these?

  #forEach: ->

  #every: ->

  #some: ->

  #filter: ->

  #map: ->

  #reduce: ->

  #reduceRight: ->
