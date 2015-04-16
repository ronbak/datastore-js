# An event indicating an update to the list of datastores.
#
# Use ```DatastoreManager.datastoreListChanged.addListener()``` to
# receive these events.
class Dropbox.Datastore.DatastoreListChanged
  # @private
  constructor: (@_dsinfos) ->

  # @private (not really private, but documenting this is not worth the space it takes)
  toString: ->
    return "Datastore.DatastoreListChanged(#{@_dsinfos.length} datastores)"

  # Returns a list containing current
  # {Dropbox.Datastore.DatastoreInfo} objects for all datastores that
  # your app can access.
  #
  # @return {Array<String>}
  getDatastoreInfos: ->
    @_dsinfos
