mock_server_m = require '../../ds_helpers/mock_server'
{MockServer, MockClient} = mock_server_m

make_fresh_record = (cb) ->
  THE_DSID = MockServer.THE_DSID

  mock_server = new MockServer
  client = new MockClient 'local', mock_server
  ds_mgr = new Dropbox.Datastore.DatastoreManager client
  ds_mgr.openDatastore THE_DSID, (err, ds) ->
    return cb err if err
    table = ds.getTable 't'
    record = table.getOrInsert 'r', {}
    cb null, ds, record

describe 'list', ->
  beforeEach (done) ->
    # TODO(dropbox): a bit of a hack, but if we don't use a new
    # server occasionally, then we can accumulate a lot of unsynced
    # deltas
    make_fresh_record (err, ds, record) =>
      throw err if err # should be impossible
      @datastore = ds
      @record = record
      @updates = []

      old_store_update = (@record._storeUpdate.bind @record)
      @record._storeUpdate = (update) =>
        @updates.push update
        old_store_update update
      done()

  describe 'creation', ->
    it 'works on empty field', ->
      list = @record.getOrCreateList 'f'
      expect(list).to.be.instanceOf Dropbox.Datastore.List
      expect(list.length()).to.equal 0

    it 'works on field with existing list', ->
      @record.set 'f', ['a', 'b']
      old_list = new Dropbox.Datastore.List @datastore, @record, 'f'
      list = @record.getOrCreateList 'f'
      expect(list).to.be.instanceOf Dropbox.Datastore.List
      expect(list.length()).to.equal 2

      # test that it updates correctly
      old_list.move 0, 1
      expect(list.toArray()).to.deep.equal ['b', 'a']

    it 'throws on existing non-list', ->
      @record.set 'f', 100
      expect(=> @record.getOrCreateList 'f').to.throw()

  describe 'with two elements', ->
    beforeEach ->
      @array = ['a', 'b']
      @record.set 'f', @array
      @updates = []
      @list = new Dropbox.Datastore.List @datastore, @record, 'f'

    describe 'move', ->
      it 'works', ->
        @list.move 0, 1
        expect(@list.toArray()).to.deep.equal ['b', 'a']
        expect(@updates).to.deep.equal [{ f: ['LM', 0, 1] }]

      it 'still works', ->
        @list.move 1, 0
        expect(@list.toArray()).to.deep.equal ['b', 'a']
        expect(@updates).to.deep.equal [{ f: ['LM', 1, 0] }]

      it 'handles index wraparounds', ->
        @list.move 0, -1
        expect(@list.toArray()).to.deep.equal ['b', 'a']
        expect(@updates).to.deep.equal [{ f: ['LM', 0, 1] }]

      it 'handles more index wraparounds', ->
        @list.move -1, -1
        expect(@list.toArray()).to.deep.equal ['a', 'b']
        expect(@updates).to.deep.equal []

      it 'handles even more index wraparounds', ->
        @list.move -1, 0
        expect(@list.toArray()).to.deep.equal ['b', 'a']
        expect(@updates).to.deep.equal [{ f: ['LM', 1, 0] }]

    describe 'remove', ->
      it 'works', ->
        expect(@list.remove 0).to.equal 'a'
        expect(@list.toArray()).to.deep.equal ['b']
        expect(@updates).to.deep.equal [{ f: ['LD', 0] }]

  describe 'with three elements', ->
    beforeEach ->
      @array = ['a', 'b', 'c']
      @record.set 'f', @array
      @updates = []
      @list = new Dropbox.Datastore.List @datastore, @record, 'f'

    it 'handles index wraparounds', ->
      @list.move -2, 2
      expect(@list.toArray()).to.deep.equal ['a', 'c', 'b']
      expect(@updates).to.deep.equal [{ f: ['LM', 1, 2] }]

    it 'handles more index wraparounds', ->
      @list.move -3, 2
      expect(@list.toArray()).to.deep.equal ['b', 'c', 'a']
      expect(@updates).to.deep.equal [{ f: ['LM', 0, 2] }]

  describe 'with five elements', ->
    beforeEach ->
      @array = ['a', 'b', 'c', 'd', 'e']
      @record.set 'f', @array
      @updates = []
      @list = new Dropbox.Datastore.List @datastore, @record, 'f'

    for i in [-5..4]
      for j in [-5..4]
        do (i, j) ->
          it "handles a move from #{i} to #{j}", ->
            x = @list.get i
            @list.move i, j
            expect(@list.get j).to.equal x
            fi = if i >= 0 then i else 5 + i
            fj = if j >= 0 then j else 5 + j
            if fi != fj
              expect(@updates).to.deep.equal [{ f: ['LM', fi, fj] }]
            else
              expect(@updates).to.deep.equal []
            arr = @list.toArray()
            value = arr[fj]
            arr.splice fj, 1
            arr.splice fi, 0, value
            expect(arr).to.deep.equal ['a', 'b', 'c', 'd', 'e']

  describe 'that has been deleted', ->
    beforeEach ->
      @record.set 'f', ['a', 'b', 'c']
      @updates = []
      @list = new Dropbox.Datastore.List @datastore, @record, 'f'

    testMethodsThrow = (expect) ->
      expect(=> @list.get(1)).to.throw()
      expect(=> @list.set(1, 'd')).to.throw()
      expect(=> @list.length()).to.throw()
      expect(=> @list.pop()).to.throw()
      expect(=> @list.push()).to.throw()
      expect(=> @list.shift()).to.throw()
      expect(=> @list.unshift('d')).to.throw()
      expect(=> @list.splice(0, 2, 'd', 'e')).to.throw()
      expect(=> @list.move(0, 2)).to.throw()
      expect(=> @list.remove(0)).to.throw()
      expect(=> @list.insert(2, 'd')).to.throw()
      expect(=> @list.slice(1, 3)).to.throw()
      expect(=> @list.toArray()).to.throw()

    it 'throws on field deleted', ->
      @record.set 'f', null
      testMethodsThrow.apply @, [expect]

    it 'throws on field replaced with number', ->
      @record.set 'f', 5
      testMethodsThrow.apply @, [expect]
