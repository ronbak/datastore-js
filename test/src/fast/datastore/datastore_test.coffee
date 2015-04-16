impl = Dropbox.Datastore.impl
Delta = impl.Delta

class FakeClient
  constructor: (snapshotReturn = { rev: 0, rows: [], role: impl.ROLE_OWNER }) ->
    @_snapshotReturn = snapshotReturn

  isAuthenticated: -> true

  _datastoreAwait: ->
    # TODO: do something

  _getOrCreateDatastore: (dsid, cb) ->
    cb null, { handle: "handle", created: true }

  _createDatastore: (dsid, key, cb) ->
    cb null, { handle: "handle", created: true }

  _getSnapshot: (handle, cb) ->
    cb null, @_snapshotReturn

  _putDelta: (handle, delta, cb) ->
    # Most of these tests happen to rely on the server not acknowledging incoming deltas.
    if @putDeltaReturn?
      cb null, @putDeltaReturn

describe 'Dropbox.Datastore', ->
  beforeEach (next) ->
    @client = new FakeClient
    @datastoreManager = new Dropbox.Datastore.DatastoreManager @client
    @datastoreManager.openDefaultDatastore (err, datastore) =>
      @datastore = datastore
      throw err if err
      @table = @datastore.getTable 'table'
      next()

  # TODO: test what happens when remote deletes a record and
  # re-creates a record with the same id.  should probably get two
  # record objects in the RecordsChanged notification?

  it 'disallows opening the same datastore multiple times', ->
    expect(=>
      @datastoreManager.openDefaultDatastore (err, datastore) =>
        throw new Error "should not have been allowed"
    ).to.throw "Attempt to open datastore multiple times"

  it 'allows reopening after closing', (done) ->
    @datastore.close()
    # expect not to throw
    @datastoreManager.openDefaultDatastore (err, datastore) =>
      done()

  it 'RecordsChanged has a decent toString', (done) ->
    @datastore.recordsChanged.addListener (e) ->
      expect("" + e).to.equal "Datastore.RecordsChanged(1 record in 1 table changed locally)"
      done()
    @table.insert {}


  # TODO: test that uploads still finish after closing a datastore


  describe 'isValidId', ->
    # Array(n+1).join(s) repeats s n times.
    GOOD_DSIDS = ['1', 'foo', Array(65).join('x'), 'foo.bar', 'foo...bar', '-foo-bar-', '_foo_bar_', '.Foo09-_bar']
    BAD_DSIDS = ['', 'A', 'foo@bar.com', Array(66).join('x'), '.', 'foo.', '.foo.bar']

    GOOD_IDS = ['1', 'foo', 'Foo-_+.=Bar', Array(65).join('x'), ':foo', ':' + Array(64).join('x')]
    BAD_IDS = ['', 'foo@bar.com', Array(66).join('x'), ':', ':' + Array(65).join('x')]

    it 'accepts good datastore names', ->
      for id in GOOD_DSIDS
        expect(Dropbox.Datastore.isValidId(id)).to.equal true
        expect(Dropbox.Datastore.isValidShareableId(id)).to.equal (id.slice(0, 1) == '.')
    it 'rejects bad datastore names', ->
      for id in BAD_DSIDS
        expect(Dropbox.Datastore.isValidId(id)).to.equal false
        expect(Dropbox.Datastore.isValidShareableId(id)).to.equal false

    it 'accepts good table names', ->
      for id in GOOD_IDS
        expect(Dropbox.Datastore.Table.isValidId(id)).to.equal true
    it 'rejects bad table names', ->
      for id in BAD_IDS
        expect(Dropbox.Datastore.Table.isValidId(id)).to.equal false

    it 'accepts good record names', ->
      for id in GOOD_IDS
        expect(Dropbox.Datastore.Record.isValidId(id)).to.equal true
    it 'rejects bad record names', ->
      for id in BAD_IDS
        expect(Dropbox.Datastore.Record.isValidId(id)).to.equal false

  describe 'getTitle', ->
    it 'correctly gets the title', ->
      @datastore.setTitle 'title'
      expect(@datastore.getTitle()).to.equal 'title'
    it 'returns null if wasn\'t set', ->
      expect(@datastore.getTitle()).to.equal null
    it 'handles deletions of the info record', ->
      @datastore.setTitle 'title'
      record = @datastore.getTable(':info').get("info")
      record.deleteRecord()
      expect(@datastore.getTitle()).to.equal null
    it 'gets title correctly from the server', ->
      managed_datastore = @datastore._managed_datastore
      sync_state = managed_datastore.sync_state
      sync_state.receive_server_delta new Delta { rev: 0, nonce: 'not_ours', changes: [
        ['I', ':info', 'info', { title: 'serverTitle' }]
      ] }
      @datastore._sync()
      expect(@datastore.getTitle()).to.equal 'serverTitle'

  describe 'sharing behaviors for private datastores', ->
    it 'returns dummy values from basic sharing inquiries', ->
      expect(@datastore.isShareable()).to.equal false
      expect(@datastore.getEffectiveRole()).to.equal Dropbox.Datastore.OWNER
      expect(@datastore.isWritable()).to.equal true

    it 'throws on all advanced sharing calls', ->
      expect(=> @datastore.getRole Dropbox.Datastore.PUBLIC).to.throw()
      expect(=> @datastore.setRole Dropbox.Datastore.PUBLIC, Dropbox.Datastore.EDITOR).to.throw()
      expect(=> @datastore.deleteRole Dropbox.Datastore.PUBLIC).to.throw()
      expect(=> @datastore.listRole()).to.throw()

  describe 'getSize', ->
    beforeEach ->
      @empty_size = @datastore.getSize()

    it 'gets the empty datastore size correctly', ->
      # An empty datastore will have an ":info" table with an "mtime" field.
      size = (Dropbox.Datastore.BASE_DATASTORE_SIZE +
              Dropbox.Datastore.Record.BASE_RECORD_SIZE +
              Dropbox.Datastore.Record.BASE_FIELD_SIZE)
      expect(@empty_size).to.equal size

    it 'calculates the size of local changes', ->
      record = @table.insert { "foo": [1, 2, 3], "bar": 42 }
      record_size = (Dropbox.Datastore.Record.BASE_RECORD_SIZE +
                     2 * Dropbox.Datastore.Record.BASE_FIELD_SIZE +
                     3 * Dropbox.Datastore.List.BASE_ITEM_SIZE)
      expect(record.getSize()).to.equal record_size
      expect(@datastore.getSize()).to.equal @empty_size + record_size

      record.update { "bar": 0 }
      expect(record.getSize()).to.equal record_size

      record.update { "bar": 0 }
      expect(record.getSize()).to.equal record_size

      record.deleteRecord()
      expect(@datastore.getSize()).to.equal @empty_size

    it 'calculates the size of remote changes', ->
      managed_datastore = @datastore._managed_datastore
      sync_state = managed_datastore.sync_state

      sync_state.receive_server_delta new Delta { rev: 0, nonce: 'not_ours', changes: [
        ['I', 'table', 'record', { foo: 42 }]
      ] }
      @datastore._sync()

      size = (@empty_size +
              Dropbox.Datastore.Record.BASE_RECORD_SIZE +
              Dropbox.Datastore.Record.BASE_FIELD_SIZE)
      expect(@datastore.getSize()).to.equal size

      sync_state.receive_server_delta new Delta { rev: 1, nonce: 'not_ours', changes: [
        ['D', 'table', 'record']
      ] }
      @datastore._sync()

      expect(@datastore.getSize()).to.equal @empty_size

  describe 'getRecordCount', ->
    it 'gets the record count correctly', ->
      # "mtime" in the ":info" table.
      expect(@datastore.getRecordCount()).to.equal 1

  describe 'with one record insertion', ->
    beforeEach ->
      # HACK(dropbox): forced it to not update the mtime
      @datastore._managed_datastore._update_mtime_on_change = false
      @record = @table.insert {}

    # TODO(dropbox): this should produce changes once the server allows that.
    it 'allows redundant deletions without producing outgoing changes', ->
      # 1 unsynced change: insert the record
      expect(@datastore._managed_datastore.sync_state.unsynced_deltas.length).to.equal 2
      expect(@datastore._managed_datastore.sync_state.unsynced_deltas[1].changes.length).to.equal 1
      expect(@record.get 'foo').to.equal null

      @record.set 'foo', null
      # nothing should happen after setting foo to null
      expect(@datastore._managed_datastore.sync_state.unsynced_deltas.length).to.equal 2
      expect(@datastore._managed_datastore.sync_state.unsynced_deltas[1].changes.length).to.equal 1
      expect(@record.get 'foo').to.equal null

      @record.set 'foo', 1
      # length should increase
      expect(@datastore._managed_datastore.sync_state.unsynced_deltas.length).to.equal 2
      expect(@datastore._managed_datastore.sync_state.unsynced_deltas[1].changes.length).to.equal 2
      expect(@record.get 'foo').to.equal 1

      @record.set 'foo', null
      expect(@datastore._managed_datastore.sync_state.unsynced_deltas.length).to.equal 2
      expect(@datastore._managed_datastore.sync_state.unsynced_deltas[1].changes.length).to.equal 3
      expect(@record.get 'foo').to.equal null

      @record.set 'foo', null
      expect(@datastore._managed_datastore.sync_state.unsynced_deltas.length).to.equal 2
      expect(@datastore._managed_datastore.sync_state.unsynced_deltas[1].changes.length).to.equal 3
      expect(@record.get 'foo').to.equal null

  it 'handles redundant incoming field deletions', ->
    managed_datastore = @datastore._managed_datastore
    sync_state = managed_datastore.sync_state
    expect(managed_datastore.get_incoming_delta_count()).to.equal 0

    sync_state.receive_server_delta new Delta { rev: 0, nonce: 'not_ours', changes: [
      ['I', 'table', 'record', { foo: 'bar' }]
    ] }
    expect(managed_datastore.get_incoming_delta_count()).to.equal 1
    expect(@table.get 'record').to.equal null
    @datastore._sync()
    expect(managed_datastore.get_incoming_delta_count()).to.equal 0
    expect((@table.get 'record').getFields()).to.deep.equal { foo: 'bar' }

    sync_state.receive_server_delta new Delta { rev: 1, nonce: 'not_ours', changes: [
      ['U', 'table', 'record', { foo: ['D'], bar: ['D'] }]
    ] }
    @datastore._sync()
    expect((@table.get 'record').getFields()).to.deep.equal {}
    sync_state.receive_server_delta new Delta { rev: 2, nonce: 'not_ours', changes: [
      ['U', 'table', 'record', { foo: ['D'], bar: ['D'] }]
      ['U', 'table', 'record', { foo: ['D'], bar: ['D'] }]
      ['U', 'table', 'record', { foo: ['D'], bar: ['D'] }]
    ] }
    @datastore._sync()
    expect((@table.get 'record').getFields()).to.deep.equal {}


  it 'allows colons in table and record ids in incoming changes', ->
    managed_datastore = @datastore._managed_datastore
    sync_state = managed_datastore.sync_state

    sync_state.receive_server_delta new Delta { rev: 0, nonce: 'not_ours', changes: [
      ['I', ':table', ':record', { ':foo': 'bar', ':bar': 'foo' }]
    ] }
    expect(@datastore.getTable(':table').get(':record')).to.equal null
    @datastore._sync()
    expect(@datastore.getTable(':table').get(':record').getFields()).to.deep.equal { ':foo': 'bar', ':bar': 'foo' }

    sync_state.receive_server_delta new Delta { rev: 1, nonce: 'not_ours', changes: [
      ['U', ':table', ':record', { ':foo': ['P', 'baz'], ':baz': ['P', 'foo'], ':bar': ['D'] }]
    ] }
    @datastore._sync()
    expect(@datastore.getTable(':table').get(':record').getFields()).to.deep.equal { ':foo': 'baz', ':baz': 'foo' }


describe 'Dropbox.Datastore for shareable datastores', ->
  beforeEach (next) ->
    @client = new FakeClient
    @datastoreManager = new Dropbox.Datastore.DatastoreManager @client
    @datastoreManager.createDatastore (err, datastore) =>
      @datastore = datastore
      throw err if err
      next()

  it 'supports basic sharing inquiries', ->
    expect(@datastore.isShareable()).to.equal true
    expect(@datastore.getEffectiveRole()).to.equal Dropbox.Datastore.OWNER
    expect(@datastore.isWritable()).to.equal true

  it 'supports advanced sharing inquiries', ->
    expect(@datastore.getRole Dropbox.Datastore.TEAM).to.equal Dropbox.Datastore.NONE
    expect(@datastore.getRole Dropbox.Datastore.PUBLIC).to.equal Dropbox.Datastore.NONE
    expect(@datastore.listRoles()).to.deep.equal({})

  it 'supports ACL changes', ->
    @datastore.setRole Dropbox.Datastore.TEAM, Dropbox.Datastore.EDITOR
    @datastore.setRole Dropbox.Datastore.PUBLIC, Dropbox.Datastore.VIEWER
    @datastore._sync()
    
    expect(@datastore.getRole Dropbox.Datastore.TEAM).to.equal Dropbox.Datastore.EDITOR
    expect(@datastore.getRole Dropbox.Datastore.PUBLIC).to.equal Dropbox.Datastore.VIEWER
    expected = {}
    expected[Dropbox.Datastore.TEAM] = Dropbox.Datastore.EDITOR
    expected[Dropbox.Datastore.PUBLIC] = Dropbox.Datastore.VIEWER
    expect(@datastore.listRoles()).to.deep.equal expected

    @datastore.setRole Dropbox.Datastore.TEAM, Dropbox.Datastore.NONE
    @datastore.deleteRole Dropbox.Datastore.PUBLIC
    expect(@datastore.listRoles()).to.deep.equal {}

describe 'Dropbox.Datastore support for other effective roles', ->
  it 'works for editor', ->
    @client = new FakeClient { rev: 0, rows: [], role: impl.ROLE_EDITOR }
    @datastoreManager = new Dropbox.Datastore.DatastoreManager @client
    @datastoreManager.createDatastore (err, datastore) =>
      @datastore = datastore
      throw err if err
      expect(@datastore.getEffectiveRole()).to.equal Dropbox.Datastore.EDITOR

      # Cursory check that we can still make changes
      @datastore.setRole Dropbox.Datastore.TEAM, Dropbox.Datastore.VIEWER
      @datastore._sync()
      t = @datastore.getTable 'table'
      t.insert {}
      # :acl should not be listed as a table ID
      expect(@datastore.listTableIds()).to.deep.equal ['table']

  it 'works for viewer', ->
    @client = new FakeClient { rev: 1, rows: [ { tid: "t1", rowid: "r1", data: {} } ], role: impl.ROLE_VIEWER }
    @datastoreManager = new Dropbox.Datastore.DatastoreManager @client
    @datastoreManager.createDatastore (err, datastore) =>
      @datastore = datastore
      throw err if err
      expect(@datastore.getEffectiveRole()).to.equal Dropbox.Datastore.VIEWER

      # Check that we can still read stuff
      expect(@datastore.getTitle()).to.equal(null)
      expect(@datastore.getModifiedTime()).not.to.equal(null)
      expect(@datastore.getRole(Dropbox.Datastore.TEAM)).to.equal(Dropbox.Datastore.NONE)
      expect(@datastore.getRole(Dropbox.Datastore.PUBLIC)).to.equal(Dropbox.Datastore.NONE)
      
      # Check that we can't make any changes
      expect(=> @datastore.setRole Dropbox.Datastore.TEAM, Dropbox.Datastore.VIEWER).to.throw()
      expect(=> @datastore.deleteRole Dropbox.Datastore.TEAM).to.throw()
      expect(=> @datastore.setTitle 'xyzzy').to.throw()
      t1 = @datastore.getTable 't1'
      expect(=> t1.insert {}).to.throw()
      expect(=> t1.getOrInsert 'r2', {}).to.throw()
      r1 = t1.get('r1')
      expect(=> r1.update { f1: 42 }).to.throw()
      expect(=> r1.set 'f1', 42).to.throw()
      expect(=> r1.deleteRecord()).to.throw()

describe 'Dropbox.Datastore handling of access_denied errors', ->
  it 'works on the initial mtime change at creation', (done) ->
    @client = new FakeClient { rev: 0, rows: [], role: impl.ROLE_EDITOR }
    @client.putDeltaReturn = { access_denied: "Test1" }
    @datastoreManager = new Dropbox.Datastore.DatastoreManager @client
    @datastoreManager.openOrCreateDatastore '.dsid', (err, datastore) =>
      expect(datastore.getEffectiveRole()).to.equal Dropbox.Datastore.VIEWER
      expect(datastore.getSyncStatus()['uploading']).to.equal false
      done()

  it 'works on a later change', (done) ->
    @client = new FakeClient { rev: 0, rows: [], role: impl.ROLE_EDITOR }
    @client.putDeltaReturn = { rev: 1 }  # For the initial mtime update
    @datastoreManager = new Dropbox.Datastore.DatastoreManager @client
    @datastoreManager.openOrCreateDatastore '.dsid', (err, datastore) =>
      expect(datastore.getEffectiveRole()).to.equal Dropbox.Datastore.EDITOR
      expect(datastore.getSyncStatus()['uploading']).to.equal false
      @client.putDeltaReturn = { access_denied: "Test2" }
      datastore.recordsChanged.addListener (e) ->
        for r in e.affectedRecordsForTable('t1')
          if r.isDeleted()
            done()
            break
      datastore.getTable('t1').getOrInsert('r1', { f1: 42 })
