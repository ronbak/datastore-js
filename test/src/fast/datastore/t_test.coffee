T = Dropbox.Datastore.impl.T

class Foo
  # pass

class Bar
  toString: -> 'bar'

describe "T.safe_to_string", ->

  it "works", ->
    expect(T.safe_to_string new Bar).to.equal 'bar'
    x = new Foo
    x.y = 5
    expect(T.safe_to_string x).to.equal '{"y":5}'

    x.y = x
    expect(T.safe_to_string x).to.equal 'Foo'

    x.constructor = {}
    expect(T.safe_to_string x).to.equal '[T.safe_to_string failed]'


describe 'T.arrayOf', ->

  it 'fromJSON passes through simple values', ->
    type = T.arrayOf T.uint
    expect(type.fromJSON [5]).to.deep.equal [5]


describe 'T.simple_typed_map T.arrayOf T.uint', ->
  beforeEach ->
    @type = T.simple_typed_map 'test map type', T.string, T.arrayOf T.uint

  it 'fromJSON works in a simple case', ->
    expect(@type.fromJSON {a: [5]}).to.deep.equal {a: [5]}
