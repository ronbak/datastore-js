buildClientTests = (clientKeys) ->
  # Creates the global client.
  setupClient = (test, done) ->
    # Should only be used for fixture teardown.
    test.__client = new Dropbox.Client clientKeys
    done()

  # Creates a test datastore.
  setupDatastore = (test, done) ->
    test.testStore = "jstest" + Math.random().toString(36).substring(2)
    test.__client._getDatastore path: test.testStore, (error, stat) ->
      expect(error).to.equal null
      test.testStoreDsid = stat.dsid
      done()

  # Populates the test datastore.
  setupData = (test, done) ->
    test.testDelta0 = Dropbox.Datastore.Delta.parse(
      rev: 0,
      metadata: 'first delta metadata',
      nonce: 'delta1-nonce',
      changes: [
        ['I', 'shapes', 'ball', { type: 'circle', color: 'red' }],
        ['I', 'shapes', 'bulb', { type: 'circle', color: 'white' }],
      ])
    test.testDelta1 = Dropbox.Datastore.Delta.parse(
      rev: 1,
      metadata: 'second delta metadata',
      nonce: 'delta2-nonce',
      changes: [
        ['I', 'classes', 'shape', { super: 'object' }],
        ['I', 'shapes', 'dropbox', { type: 'box', color: 'blue' }],
      ])
    test.__client.putDelta test.testStoreDsid, test.testDelta0, (error) ->
      expect(error).to.equal null
      test.__client.putDelta test.testStoreDsid, test.testDelta1, (error) ->
        expect(error).to.equal null
        done()

  # Global (expensive) fixtures.
  before (done) ->
    setupClient @, =>
      setupDatastore @, =>
        setupData @, ->
          done()

  # Teardown for global fixtures.
  after (done) ->
    @__client.deleteDatastore path: @testStore, (error, stat) =>
      throw new Error(error) if error
      done()

  # Per-test (cheap) fixtures.
  beforeEach ->
    @client = new Dropbox.Client clientKeys


  describe '#listDatastores', ->
    it 'returns an array that includes the test store', (done) ->
      @client.listDatastores (error, stats) =>
        expect(error).to.equal null
        expect(stats).to.have.property 'length'
        expect(stats.length).to.be.above 0
        testStat = null
        for stat in stats
          if stat.path is @testStore
            testStat = stat
            break
        expect(testStat).not.to.equal null
        expect(testStat.dsid).to.equal @testStoreDsid
        done()

  describe '#getDatastore', ->
    describe 'with noCreate: true', ->
      it 'returns an API error for a new datastore', (done) ->
        options = path: @testStore + '00no', noCreate: true
        @client.getDatastore options, (error, stat) ->
          expect(stat).not.to.be.ok
          expect(error).to.be.instanceOf Dropbox.ApiError
          expect(error.status).to.equal Dropbox.ApiError.NOT_FOUND
          done()

      it 'finds an existing datastore', (done) ->
        @client.getDatastore path: @testStore, noCreate: true, (error, stat) =>
          expect(error).to.equal null
          expect(stat).to.be.instanceOf Dropbox.Datastore.Stat
          expect(stat.path).to.equal @testStore
          expect(stat.dsid).to.equal @testStoreDsid
          done()

    describe 'with path', ->
      it 'finds an existing datastore', (done) ->
        @client.getDatastore path: @testStore, (error, stat) =>
          expect(error).to.equal null
          expect(stat).to.be.instanceOf Dropbox.Datastore.Stat
          expect(stat.path).to.equal @testStore
          expect(stat.dsid).to.equal @testStoreDsid
          done()

      it 'creates datastores on-demand', (done) ->
        newStore = @testStore + '00new'
        @client.getDatastore path: newStore, (error, stat) =>
          expect(error).to.equal null
          expect(stat).to.be.instanceOf Dropbox.Datastore.Stat
          expect(stat.path).to.equal newStore
          expect(stat.dsid).not.to.equal @testStoreDsid
          done()

    describe 'with no options', ->
      it 'opens the default datastore', (done) ->
        @client.getDatastore (error, stat) ->
          expect(error).to.equal null
          expect(stat).to.be.instanceOf Dropbox.Datastore.Stat
          expect(stat.path).to.equal 'default'
          done()

  describe '#deleteDatastore', ->
    describe 'with an existing datastore', ->
      beforeEach (done) ->
        @newStore = @testStore + '00del'
        @client.getDatastore path: @newStore, (error, stat) ->
          expect(error).to.equal null
          expect(stat).to.be.ok
          done()
      afterEach (done) ->
        @client.deleteDatastore path: @newStore, (error) ->
          done()

      it 'removes the datastore', (done) ->
        @client.deleteDatastore path: @newStore, (error) =>
          expect(error).to.equal null
          @client.getDatastore(
              path: @newStore, noCreate: true, (error, stat) =>
                expect(stat).not.to.be.ok
                expect(error).to.be.instanceOf Dropbox.ApiError
                expect(error.status).to.equal Dropbox.ApiError.NOT_FOUND
                done()
          )

      it 'returns an API error for a non-existing datastore', (done) ->
        @client.deleteDatastore path: @testStore + '00no2', (error) =>
          expect(error).to.be.instanceOf Dropbox.ApiError
          expect(error.status).to.equal Dropbox.ApiError.NOT_FOUND
          done()

  describe '#getDeltas', ->
    # TODO(pwnall): enable test when the API server gets fixed
    it.skip 'returns an API error for non-existing datastore', (done) ->
      @client.getDeltas dsid: 'no-such-dsid', revision: 0,
          (error, sequence, more) ->
            expect(sequence).not.to.be.ok
            expect(error).to.be.instanceOf Dropbox.ApiError
            expect(error.status).to.equal Dropbox.ApiError.NOT_FOUND
            done()

    # disabled because (a) we don't need get_deltas, and (b) handles make this more complicated
    #describe 'starting from revision 0', ->
    #  it 'gets all deltas', (done) ->
    #    @client.getDeltas dsid: @testStoreDsid, revision: 0,
    #        (error, resp) =>
    #          expect(error).to.equal null
    #          expect(resp.deltas.length).to.equal 2
    #          delta0 = resp.deltas[0]
    #          expect(delta0).to.have.property 'rev'
    #          expect(delta0.rev).to.equal 0
    #          expect(delta0).to.have.property 'changes'
    #          expect(delta0.changes).to.deep.equal @testDelta0.changes
    #          delta1 = sequence.deltas[1]
    #          expect(delta1).to.have.property 'rev'
    #          expect(delta1.revision).to.equal 1
    #          expect(delta1).to.have.property 'changes'
    #          expect(delta1.changes).to.deep.equal @testDelta1.changes
    #          done()
    #
    #describe 'starting from revision 1', ->
    #  it 'gets the last delta', (done) ->
    #    @client.getDeltas dsid: @testStoreDsid, revision: 1,
    #        (error, resp) =>
    #          expect(error).to.equal null
    #          expect(resp.deltas.length).to.equal 1
    #          delta1 = resp.deltas[0]
    #          expect(delta1).to.have.property 'rev'
    #          expect(delta1.rev).to.equal 1
    #          expect(delta1).to.have.property 'changes'
    #          expect(delta1.changes).to.deep.equal @testDelta1.changes
    #          done()
    #
    #describe 'starting from revision 2', ->
    #  it 'gets no deltas', (done) ->
    #    @client.getDeltas dsid: @testStoreDsid, revision: 2,
    #        (error, sequence, more) ->
    #          expect(error).to.equal null
    #          expect(sequence).to.be.instanceOf Dropbox.Datastore.DeltaSequence
    #          expect(sequence.deltas().length).to.equal 0
    #          expect(more).to.equal false
    #          done()

  describe '#putDelta', (done) ->
    # TODO(pwnall): enable test when the API server gets fixed
    it.skip 'returns an API error for non-existing datastore', (done) ->
      @client.putDelta 'no-such-dsid', @testDelta0, (error, deltas, more) ->
        expect(error).to.be.instanceOf Dropbox.ApiError
        expect(error.status).to.equal Dropbox.ApiError.NOT_FOUND
        done()

    describe 'on an empty database', (done) ->
      beforeEach (done) ->
        @newStore = @testStore + '00put'
        @client.getDatastore path: @newStore, (error, stat) =>
          expect(error).not.to.be.ok
          expect(stat.revision).to.equal 0
          @storeDsid = stat.dsid
          done()
      afterEach (done) ->
        @client.deleteDatastore path: @newStore, (error) =>
          expect(error).not.to.be.ok
          done()

      it 'succesfully submits a delta baselined at 0', (done) ->
        @client.putDelta @storeDsid, @testDelta0, (error) =>
          expect(error).to.equal null
          @client.getDeltas dsid: @storeDsid, revision: 0,
              (error, sequence, more) =>
                expect(error).to.equal null
                expect(sequence.deltas().length).to.equal 1
                delta0 = sequence.deltas()[0]
                expect(delta0.revision).to.equal 0
                expect(delta0.metadata).to.equal 'first delta metadata'
                expect(delta0.changes).to.deep.equal @testDelta0.changes
                expect(more).to.equal false
                done()

      # TODO(pwnall): enable test when the API server gets fixed
      it.skip 'returns an API error on a delta baselined at 1', (done) ->
        @client.putDelta @storeDsid, @testDelta1, (error) =>
          expect(error).to.be.instanceOf Dropbox.ApiError
          expect(error.status).to.equal Dropbox.ApiError.INVALID_PARAM
          done()

    describe 'on a database with 1 revision', ->
      beforeEach (done) ->
        @newStore = @testStore + '00put'
        @client.getDatastore path: @newStore, (error, stat) =>
          expect(error).not.to.be.ok
          expect(stat.revision).to.equal 0
          @storeDsid = stat.dsid
          @client.putDelta @storeDsid, @testDelta0, (error) =>
            expect(error).not.to.be.ok
            done()
      afterEach (done) ->
        @client.deleteDatastore path: @newStore, (error) =>
          expect(error).not.to.be.ok
          done()

      it 'succesfully submits a delta baselined at 1', (done) ->
        @client.putDelta @storeDsid, @testDelta1, (error) =>
          expect(error).to.equal null
          @client.getDeltas dsid: @storeDsid, revision: 1,
              (error, sequence, more) =>
                expect(error).to.equal null
                expect(sequence.deltas().length).to.equal 1
                delta1 = sequence.deltas()[0]
                expect(delta1.revision).to.equal 1
                expect(delta1.metadata).to.equal 'second delta metadata'
                expect(delta1.changes).to.deep.equal @testDelta1.changes
                expect(more).to.equal false
                done()

      it 'returns an API error on another delta baselined at 0', (done) ->
        delta = Dropbox.Datastore.Delta.parse(
          rev: @testDelta0.revision,
          metadata: @testDelta0.metadata,
          changes: @testDelta0.changes,
          nonce: 'xxzzyy')
        @client.putDelta @storeDsid, delta, (error) =>
          expect(error).to.be.instanceOf Dropbox.ApiError
          expect(error.status).to.equal Dropbox.ApiError.CONFLICT
          done()

      # TODO(pwnall): enable test when the API server gets fixed
      it.skip 'succesfully re-submits the same delta baselined at 0', (done) ->
        @client.putDelta @storeDsid, @testDelta0, (error) =>
          expect(error).to.equal null
          @client.getDeltas dsid: @storeDsid, revision: 0,
              (error, sequence, more) =>
                expect(error).to.equal null
                expect(sequence.deltas()).to.have.property 'length'
                expect(sequence.deltas().length).to.equal 1
                done()

  describe '#getSnapshot', ->
    # TODO(pwnall): enable test when the API server gets fixed
    it.skip 'returns an API error for a non-existing datastore', (done) ->
      @client.getSnapshot dsid: 'no-such-dsid', revision: 0, (error, snapshot) ->
        expect(snapshot).not.to.be.ok
        expect(error).to.be.instanceOf Dropbox.ApiError
        expect(error.status).to.equal Dropbox.ApiError.NOT_FOUND
        done()

    describe 'with a string datastore ID', ->
      it 'returns a correct snapshot', (done) ->
        @client.getSnapshot @testStoreDsid, (error, snapshot) =>
          expect(error).to.equal null
          expect(snapshot).to.be.instanceOf Dropbox.Datastore.Snapshot
          expect(snapshot.dsid).to.equal @testStoreDsid
          expect(snapshot.revision).to.equal 2
          recordData = for record in snapshot.records()
            [record.tid, record.rowid, record.data]
          recordData.sort()
          expect(recordData).to.deep.equal [
            ['classes', 'shape', { super: 'object' }],
            ['shapes', 'ball', { type: 'circle', color: 'red' }],
            ['shapes', 'bulb', { type: 'circle', color: 'white' }],
            ['shapes', 'dropbox', { type: 'box', color: 'blue' }],
          ]
          done()

    describe 'with a Datastore.Stat', ->
      it 'returns a correct snapshot', (done) ->
        stat = Dropbox.Datastore.Stat.parse dbid: @testStoreDsid
        @client.getSnapshot stat, (error, snapshot) =>
          expect(error).to.equal null
          expect(snapshot).to.be.instanceOf Dropbox.Datastore.Snapshot
          expect(snapshot.dsid).to.equal @testStoreDsid
          expect(snapshot.revision).to.equal 2
          recordData = for record in snapshot.records()
            [record.tid, record.rowid, record.data]
          recordData.sort()
          expect(recordData).to.deep.equal [
            ['classes', 'shape', { super: 'object' }],
            ['shapes', 'ball', { type: 'circle', color: 'red' }],
            ['shapes', 'bulb', { type: 'circle', color: 'white' }],
            ['shapes', 'dropbox', { type: 'box', color: 'blue' }],
          ]
          done()

  describe '#awaitDeltas', ->
    beforeEach ->
      @syncSet = new Dropbox.Datastore.SyncSet

    describe 'with one cursor', ->
      # TODO(pwnall): enable test when the API server gets fixed
      it.skip 'returns an API error for a non-existing datastore', (done) ->
        @syncSet.set 'no-such-dsid', 1
        @client.awaitDeltas @syncSet, (error, deltas) =>
          expect(deltas).not.to.be.ok
          expect(error).to.be.instanceOf Dropbox.ApiError
          expect(error.status).to.equal Dropbox.ApiError.NOT_FOUND
          done()

      it 'returns right away if there are changes', (done) ->
        @syncSet.set @testStoreDsid, 1
        @client.awaitDeltas @syncSet, (error, sequences) =>
          expect(error).to.equal null
          expect(sequences).to.have.property 'length'
          expect(sequences.length).to.equal 1
          sequence = sequences[0]
          expect(sequence).to.be.instanceOf Dropbox.Datastore.DeltaSequence
          expect(sequence.dsid).to.equal @testStoreDsid
          expect(sequence.deltas().length).to.equal 1
          delta1 = sequence.deltas()[0]
          expect(delta1.revision).to.equal 1
          expect(delta1.metadata).to.equal 'second delta metadata'
          expect(delta1.changes).to.deep.equal @testDelta1.changes
          done()

describe.skip 'Dropbox.Client', ->
  # Skip some of the long tests in Web workers.
  unless (typeof self isnt 'undefined') and (typeof window is 'undefined')
    describe 'with full Dropbox access', ->
      buildClientTests testFullDropboxKeys

  describe 'with Folder access', ->
    buildClientTests testKeys
