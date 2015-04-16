describe 'Dropbox.Datastore', ->

  it 'datastore deletion is detected', (done) ->
    client = new Dropbox.Client testKeys
    ds_mgr = new Dropbox.Datastore.DatastoreManager client

    ds_mgr.openDefaultDatastore (err, datastore) ->
      expect(err).to.equal null
      default_id = datastore.getId()

      ds_mgr.datastoreListChanged.addListener (e) ->
        # e is a DatastoreListChanged object
        ds_list = e.listDatastoreIds()

        # TODO: test that this listener correctly gets called when
        # datastore is deleted... if that code path fails, this test
        # never finishes
        if default_id not in ds_list
          table = datastore.getTable 't'
          fn = -> table.insert {x: 0}
          expect(fn).to.throw "Cannot sync deleted datastore #{default_id}."
          done()

      ds_mgr.deleteDatastore default_id, (err) ->
        expect(err).to.equal null
