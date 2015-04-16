mock_server_m = require '../../ds_helpers/mock_server'
{MockServer, MockClient} = mock_server_m

find_dsinfo = (dslist_response, id) ->
  for info in dslist_response
    if info.getId() == id
      return info
  return null

describe 'metadata_test listDatastores', ->
  beforeEach (next) ->
    THE_DSID = MockServer.THE_DSID
    @mock_server = new MockServer
    @client = new MockClient 'local', @mock_server
    @ds_mgr = new Dropbox.Datastore.DatastoreManager @client
    @ds_mgr.openDatastore THE_DSID, (err, datastore) =>
      @datastore = datastore
      throw err if err
      @datastore._managed_datastore._clock = => new Date(2000)
      @table = @datastore.getTable 'table'
      next()

  it 'gets info from the server when no local revisions', ->
    entry = [
      dsid: MockServer.THE_DSID
      handle: MockServer.THE_HANDLE
      rev: @mock_server.delta_list.length
      info: {'title': 'serverTitle', 'mtime': 0}
    ]
    @mock_server.setListDatastoresResponse entry
    @ds_mgr.listDatastores (error, response) =>
      info = response[0]
      expect(info.getTitle()).to.equal 'serverTitle'
      expect(info.getModifiedTime().valueOf()).to.equal 0
      expect(info.isShareable()).to.equal false
      expect(info.getEffectiveRole()).to.equal Dropbox.Datastore.OWNER
      expect(info.isWritable()).to.equal true

  it 'correctly overrides with local deltas', ->
    entry = [
      dsid: MockServer.THE_DSID
      handle: MockServer.THE_HANDLE
      rev: @mock_server.delta_list.length
      info: {'title': 'serverTitle', 'mtime': 0}
    ]
    @mock_server.setListDatastoresResponse entry
    @datastore.setTitle 'localTitle'
    @ds_mgr.listDatastores (error, response) =>
      expect(response[0].getTitle()).to.equal 'localTitle'
      expect(response[0].getModifiedTime().valueOf()).to.equal 2000

  it 'does not allow title to be an int', ->
    expect(=> @datastore.setTitle 2).to.throw /not a string/

  it 'omits a field if the field is set to null', ->
    entry = [
      dsid: MockServer.THE_DSID
      handle: MockServer.THE_HANDLE
      rev: @mock_server.delta_list.length
      info: {'title': 'serverTitle', 'mtime': 0}
    ]
    @mock_server.setListDatastoresResponse entry
    @datastore.setTitle 'tempTitle'
    @datastore.setTitle null
    @ds_mgr.listDatastores (error, response) =>
      expect(response[0].getTitle()).to.be.null

  it 'is null if record is deleted', ->
    entry = [
      dsid: MockServer.THE_DSID
      handle: MockServer.THE_HANDLE
      rev: @mock_server.delta_list.length
      info: {'title': 'serverTitle', 'mtime': 0}
    ]
    @mock_server.setListDatastoresResponse entry
    @datastore.setTitle 'tempTitle'
    @datastore.getTable(':info').get('info').deleteRecord()
    @ds_mgr.listDatastores (error, response) =>
      info = response[0]._info_record_data
      expect(info.mtime).to.exist
      expect(Object.keys(info).length).to.equal 1

  describe 'listener', ->
    beforeEach ->
      @ds_mgr._lastListDsServerResponse = null
      @ds_mgr.datastoreListChanged.addListener (event) =>
        @listener_called = true
      @listener_called = false

    it 'is called after setting the title', ->
      expect(@listener_called).to.be.false
      @datastore.setTitle 'title'
      expect(@listener_called).to.be.true
      @ds_mgr.listDatastores (error, response) ->
        expect(error).to.be.null
        info = find_dsinfo response, MockServer.THE_DSID
        expect(info).to.not.be.null
        expect(info.getTitle()).to.equal 'title'

    it 'is called when two are set', ->
      expect(@listener_called).to.be.false
      @datastore.setTitle 'title'
      expect(@listener_called).to.be.true
      @listener_called = false
      @second_listener = false
      # set the second listener after an event has dispatched
      @ds_mgr.datastoreListChanged.addListener (event) =>
        @second_listener = true
      @datastore.setTitle 'title'
      expect(@listener_called).to.be.true
      expect(@second_listener).to.be.true

    it 'is called when title is deleted', ->
      @datastore.setTitle 'title'
      @listener_called = false
      @datastore.setTitle null
      expect(@listener_called).to.be.true
      @ds_mgr.listDatastores (error, response) ->
        expect(error).to.be.null
        info = find_dsinfo response, MockServer.THE_DSID
        expect(info).to.not.be.null
        expect(info.getTitle()).to.be.null

    it 'is not called when just mtime is updated', ->
      expect(@listener_called).to.be.false
      # update a different record to update the mtime
      table = @datastore.getTable "newTable"
      prev_mtime = @datastore.getModifiedTime()
      table.insert({"fieldname":"value"});
      expect(@datastore.getModifiedTime()).to.not.equal prev_mtime
      expect(@listener_called).to.be.false

    it 'is called when there is a title and the record is deleted', ->
      @datastore.setTitle 'title'
      @listener_called = false
      @datastore.getTable(':info').get('info').deleteRecord()
      expect(@listener_called).to.be.true
      @ds_mgr.listDatastores (error, response) ->
        expect(error).to.be.null
        info = find_dsinfo response, MockServer.THE_DSID
        expect(info).to.not.be.null
        expect(info.getTitle()).to.be.null

    it 'is not called when there is no title and the record is deleted', ->
      @datastore.setTitle null
      @listener_called = false
      @datastore.getTable(':info').get('info').deleteRecord()
      expect(@listener_called).to.be.false

  it 'contains correct info for a locally created datastore', ->
    dsid = MockServer.SECOND_DSID
    @ds_mgr._getExistingDatastoreByDsid dsid, (error, datastore) =>
      datastore.setTitle "newDsTitle"
      @ds_mgr.listDatastores (error, response) =>
        expect(error).to.be.null
        info = find_dsinfo response, dsid
        expect(info).to.not.be.null
        expect(info.getTitle()).to.equal "newDsTitle"

  it 'contains correct info for a server created datastore', ->
    entries = [
      {
        dsid: MockServer.THE_DSID
        handle: MockServer.THE_HANDLE
        rev: @mock_server.delta_list.length
        info: {'title': 'title1'}
      },
      {
        dsid: MockServer.SECOND_DSID
        handle: MockServer.THE_HANDLE
        rev: @mock_server.delta_list.length
        info: {'title': 'title2'}
      }
    ]
    @mock_server.setListDatastoresResponse entries
    @ds_mgr.listDatastores (error, response) =>
      expect(error).to.be.null
      info1 = find_dsinfo response, MockServer.THE_DSID
      expect(info1).to.not.be.null
      expect(info1.getTitle()).to.equal "title1"

      info2 = find_dsinfo response, MockServer.SECOND_DSID
      expect(info2).to.not.be.null
      expect(info2.getTitle()).to.equal "title2"

  it 'uses local title if local rev is higher', ->
    entry = [
      dsid: MockServer.THE_DSID
      handle: MockServer.THE_HANDLE
      rev: 0
      info: {'title': 'serverTitle', 'mtime': 0}
    ]
    @mock_server.setListDatastoresResponse entry
    @datastore.setTitle 'localTitle'
    @datastore._managed_datastore.datastore_model.clearInfoFields()
    expect(@datastore._managed_datastore.datastore_model._changedInfoFields).to.be.empty
    @datastore._managed_datastore.sync_state._server_rev = 2
    @ds_mgr.listDatastores (error, response) =>
      expect(response[0].getTitle()).to.equal 'localTitle'
      expect(response[0].getModifiedTime().valueOf()).to.equal 2000

  it 'works for various shareable datastores', ->
    entries = [
      {
        dsid: ".dsid1"
        handle: "handle1"
        role: Dropbox.Datastore.impl.ROLE_VIEWER
        rev: 1
      },
      {
        dsid: ".dsid2"
        handle: "handle2"
        role: Dropbox.Datastore.impl.ROLE_EDITOR
        rev: 2
      },
      {
        dsid: ".dsid3"
        handle: "handle3"
        role: Dropbox.Datastore.impl.ROLE_OWNER
        rev: 3
      },
    ]
    @mock_server.setListDatastoresResponse entries
    @ds_mgr.listDatastores (error, response) =>
      expect(error).to.be.null

      info1 = find_dsinfo response, ".dsid1"
      expect(info1).to.not.be.null
      expect(info1.isShareable()).to.equal true
      expect(info1.getEffectiveRole()).to.equal Dropbox.Datastore.VIEWER
      expect(info1.isWritable()).to.equal false

      info2 = find_dsinfo response, ".dsid2"
      expect(info2).to.not.be.null
      expect(info2.isShareable()).to.equal true
      expect(info2.getEffectiveRole()).to.equal Dropbox.Datastore.EDITOR
      expect(info2.isWritable()).to.equal true

      info3 = find_dsinfo response, ".dsid3"
      expect(info3).to.not.be.null
      expect(info3.isShareable()).to.equal true
      expect(info3.getEffectiveRole()).to.equal Dropbox.Datastore.OWNER
      expect(info3.isWritable()).to.equal true

  # TODO(dropbox): uncomment test after we have redundant deletions producing outgoing changes
  # it 'handles when info set null was never existant', ->
  #   entry =
  #       info: {'title': 'serverTitle', 'mtime': 0}
  #   @mock_server.setListDatastoresResponse entry
  #   @datastore.setTitle null
  #   @ds_mgr.listDatastores (error, response) =>
  #     expect(response[0].getTitle()).to.be.null

  it 'does not list deleted datastores', ->
    dsid = MockServer.THE_DSID
    @ds_mgr.deleteDatastore dsid, (err) =>
      # Help the mocking code
      @ds_mgr._obj_manager._evict MockServer.THE_HANDLE
      @mock_server.setListDatastoresResponse []
      @ds_mgr.listDatastores (error, response) =>
        newDsExists = false
        for dsInfo in response
          if dsInfo.getId() == dsid
            newDsExists = true
        expect(newDsExists).to.equal false

  it 'uncaches a datastore that is closed and then deleted', ->
    dsid = MockServer.THE_DSID
    @datastore.close()
    @ds_mgr._obj_manager._evict MockServer.THE_HANDLE
    expect(@ds_mgr._obj_manager._cached_objects[dsid]?).to.equal false

  it 'uncaches a datastore that is deleted and then closed', ->
    dsid = MockServer.THE_DSID
    @ds_mgr._obj_manager._evict MockServer.THE_HANDLE
    @datastore.close()
    expect(@ds_mgr._obj_manager._cached_objects[dsid]?).to.equal false
