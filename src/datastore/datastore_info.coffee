# Metadata about a datastore.
class Dropbox.Datastore.DatastoreInfo
  # @private
  #
  # @_info_record_data may be null.
  constructor: (@_dsid, @_handle, @_info_record_data, @_role) ->

  # @private (not really private, but documenting this is not worth the space it takes)
  toString: ->
    return "Datastore.DatastoreInfo(#{@_dsid} #{JSON.stringify (@_info_record_data || {})} #{@getEffectiveRole()})"


  # Returns the ID of the datastore.
  #
  # @return {String}
  getId: ->
    @_dsid

  # Returns whether this datastore is shareable.
  # This is purely a function of the datastore ID.
  #
  # @return {Boolean} True if this datastore is shareable, false otherwise
  isShareable: ->
    return @_dsid[0] == '.'

  # @private (for now -- should probably be public, in the long term)
  #
  # Returns the handle of the datastore.  TODO: define what this
  # returns for offline-created datastores that aren't created on the
  # server yet.
  #
  # @return {String}
  getHandle: ->
    @_handle

  # Returns the title of the datastore, if set.
  #
  # @return {?String}
  getTitle: ->
    return null unless @_info_record_data?.title?
    return @_info_record_data.title

  # Returns the last modification time of the datastore, if set.
  #
  # @return {?Date}
  getModifiedTime: ->
    return null unless @_info_record_data?.mtime?
    return @_info_record_data.mtime

  # Returns the effective role of the datastore.
  #
  # @return {String}
  getEffectiveRole: ->
    return Dropbox.Datastore.OWNER unless @isShareable() and @_role?
    return Dropbox.Datastore._roleFromInt(@_role)

  # Returns whether this datastore is writable.  This is a shorthand
  # for testing whether {Dropbox.Datastore.DatastoreInfo#getEffectiveRole}
  # returns one of <a href="#Dropbox.Datastore.OWNER">OWNER</a> or
  # <a href="#Dropbox.Datastore.EDITOR">EDITOR</a>.
  #
  # @return {Boolean} True if this datastore is writable, false otherwise
  isWritable: () ->
    role = @getEffectiveRole()
    return role == Dropbox.Datastore.OWNER or role == Dropbox.Datastore.EDITOR
