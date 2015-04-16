{ListDatastoresResponse, EventSourceWithInitialData} = Dropbox.Datastore.impl

class MockClient
  constructor: ->
    @nextResponse = null

  _getNextResponse: ->
    x = @nextResponse
    assert x
    @nextResponse = null
    return x

  isAuthenticated: -> true

  _datastoreAwait: ->
    # nothing for now

  _getOrCreateDatastore: (dsid, cb) ->
    cb null, @_getNextResponse()

  _getDatastore: (dsid, cb) ->
    cb null, @_getNextResponse()

  _getSnapshot: (handle, cb) ->
    cb null, @_getNextResponse()

  _deleteDatastore: (handle, cb) ->
    cb null, @_getNextResponse()


describe 'DatastoreManager', ->

  it "parses list_datastores responses correctly", ->
    m = new Dropbox.Datastore.DatastoreManager {
      isAuthenticated: -> true
      _datastoreAwait: -> null
    }
    infos = m._getOverlaidDatastoreInfosFromListResponse new ListDatastoresResponse {"datastores": [{"handle": "VU8wysc6oIFXdfHHJEAuefFcjg8E40", "rev": 3, "dsid": ".UI3UDtl4k2f2uQlST-5Sf5gnuqtB_rjUPvY7DZzRiGg"}, {"handle": "COTEgE3T4UITuztSY2eNHBYx3VLI3C", "rev": 1, "dsid": "default"}], "token": "853c478e022607229fa5a6e334f9aa0114fcc8b7f5beda2687138a8a65233a8c"}
    expect(infos.length).to.equal 2
    expect(infos[0].getId()).to.equal ".UI3UDtl4k2f2uQlST-5Sf5gnuqtB_rjUPvY7DZzRiGg"
    expect(infos[1].getId()).to.equal "default"

  describe 'running with mock client', ->

    beforeEach ->
      @mockClient = new MockClient
      @datastoreManager = new Dropbox.Datastore.DatastoreManager @mockClient

    it 'openDatastore() handles datastore not found', (done) ->
      @mockClient.nextResponse = {"notfound": "it's not there"}
      @datastoreManager.openDatastore "todosv5", (err, ds) ->
        expect(ds).to.be.falsy
        expect("#{err}").to.contain("Datastore todosv5 not found or not accessible")
        done()

    it 'deleteDatastore() handles datastore not found', (done) ->
      @mockClient.nextResponse = {"notfound": "it's not there"}
      @datastoreManager.deleteDatastore "todosv5", (err) ->
        expect("#{err}").to.contain("Datastore todosv5 not found or not accessible")
        done()



describe 'DatastoreManager EventSourceWithInitialData', ->
  beforeEach ->
    @s = new EventSourceWithInitialData

  it "doesn't call the listener initially when there's no event", (done) ->
    l1 = (data) =>
      assert false, "should not be called"
    @s.addListener l1
    done()

  it "calls the listener with the initial event", (done) ->
    @s.dispatch "foo"
    l1 = (data) =>
      expect(data).to.equal "foo"
      done()
    @s.addListener l1

  it "calls the listener when event is fired", (done) ->
    l1 = (data) =>
      expect(data).to.equal "bar"
      done()
    @s.addListener l1
    @s.dispatch "bar"
