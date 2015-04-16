int64 = Dropbox.Datastore.int64
isInt64 = Dropbox.Datastore.isInt64
impl = Dropbox.Datastore.impl

# TODO: follow BDD conventions
describe 'Datastore types', ->
  # [decoded, encoded]
  BLOB_PAIRS = [
    [[251], '-w']
    [[252], '_A']
    [[1, 2, 3], 'AQID']
    [[0, 0, 0, 0, 0], 'AAAAAAA']
    [[5, 5, 5, 5, 5], 'BQUFBQU']
    [[9, 9, 9], 'CQkJ']
    [[132, 211, 32], 'hNMg']
    [[11, 26, 38], 'Cxom']
  ]

  describe 'int64', ->
    it 'from string', () ->
      x = int64 "14"
      expect(Number x).to.equal(14)
      expect(x.valueOf()).to.equal(14)
      expect(x.dbxInt64).to.equal("14")

    it 'from int', () ->
      x = int64 0
      expect(Number x).to.equal(0)
      expect(x.valueOf()).to.equal(0)
      expect(x.dbxInt64).to.equal("0")

    it 'from other int64', ->
      x = int64 int64 12
      expect(Number x).to.equal(12)
      expect(x.valueOf()).to.equal(12)
      expect(x.dbxInt64).to.equal("12")

    it 'rejects bad input', ->
      expect(-> int64 'foo').to.throw("Not a valid int64 in string form: foo")
      expect(-> int64 1.5).to.throw("Number is not an integer: 1.5")
      expect(-> int64 1e100).to.throw("Number not in int64 range: 1e+100")
      x = new Number(15)
      x.dbxInt64 = 15
      expect(-> int64 x).to.throw("Missing or invalid tag in int64: 15")
      x = new Number(15)
      x.dbxInt64 = 'a'
      expect(-> int64 x).to.throw("Missing or invalid tag in int64: a")
      x = new Number(15)
      x.dbxInt64 = '1.0'
      expect(-> int64 x).to.throw("Missing or invalid tag in int64: 1.0")
      x = new Number(15)
      x.dbxInt64 = "16"
      expect(-> int64 x).to.throw("Tag in int64 does not match value 15: 16")

  describe 'isInt64', ->
    it 'accepts int64 for small ints', ->
      expect(isInt64(int64('9999'))).to.equal true

    it 'accepts int64 for large ints', ->
      expect(isInt64(int64('9999999999999999'))).to.equal true

    it 'rejects doubles', ->
      expect(isInt64(9999)).to.equal false

    it 'rejects strings', ->
      expect(isInt64('9999')).to.equal false

    it 'rejects boxed numbers', ->
      expect(isInt64(new Number(9999))).to.equal false

    it 'rejects boxed numbers with dbxInt64 set to non-numeric stuff', ->
      boxed = new Number(9999)
      boxed.dbxInt64 = '9999a'
      expect(isInt64(boxed)).to.equal false

  describe 'toDsValue', ->
    it 'int64', ->
      expect(impl.toDsValue int64 12).to.deep.equal { I: "12" }
      expect(impl.toDsValue int64 "9223372036854775807").to.deep.equal { I: "9223372036854775807" }
      # This is probably testing implementation-defined behavior
      expect(impl.toDsValue int64 9223372036854775000).to.deep.equal { I: "9223372036854774784" }

    it 'number', ->
      expect(impl.toDsValue 12).to.equal(12)

    it 'handles uint8Array', ->
      for [decoded, encoded] in BLOB_PAIRS
        x = new Uint8Array decoded.length
        for val, idx in decoded
          x[idx] = val
        expect(impl.toDsValue x).to.deep.equal {B: encoded}

    it 'rejects functions, with a useful message', ->
      expect(-> impl.toDsValue ->).to.throw "Unexpected value: function () {}"

  describe 'fromDsValue', ->
    it 'int64', ->
      x = impl.fromDsValue null, null, null, { I: "-18" }
      expect(Number x).to.equal(-18)
      expect(x.valueOf()).to.equal(-18)
      expect(x.dbxInt64).to.equal("-18")

    it 'handles blobs', ->
      for [decoded, encoded] in BLOB_PAIRS
        x = impl.fromDsValue null, null, null, { B: encoded }
        expect(x.length).to.deep.equal decoded.length
        for val, idx in decoded
          expect(x[idx]).to.equal val

  describe 'uint8ArrayFromBase64String and base64StringFromUint8Array', ->
    it 'are inverses', ->
      str_to_uint8array = (s) ->
        arr = new Uint8Array s.length
        for i in [0...s.length]
          arr[i] = s.charCodeAt i
        return arr

      DECODED_TEST_STRINGS = ['this is a very long string.  Did you know some base64 implementations put a newline in every 73 chars??? namely base64.encodestring and base64.decodestring'
                              'weeeeeeeeeeeeeeeee here\'s another stringz'
                              'foo']
      ENCODED_TEST_STRINGS = ['foo']

      for s in DECODED_TEST_STRINGS
        arr = str_to_uint8array s
        encoded = impl.base64StringFromUint8Array arr
        decoded = impl.uint8ArrayFromBase64String encoded
        expect(arr.length).to.equal decoded.length
        for i in [0...arr.length]
          expect(arr[i]).to.equal decoded[i]

      for s in ENCODED_TEST_STRINGS
        decoded = impl.uint8ArrayFromBase64String s
        encoded = impl.base64StringFromUint8Array decoded
        expect(s).to.equal encoded
