T = Dropbox.Datastore.impl.T
struct = Dropbox.Datastore.impl.struct

describe "struct", ->

  it "works mostly", ->
    st = struct.define('st', [
      ['foo', T.string],
      ['bar', T.int],
    ])
    x = new st({foo: "a", bar: 5})
    expect(x.foo).to.equal("a")
    expect(x.bar).to.equal(5)
    expect("#{x}").to.equal('{foo: "a", bar: 5}')
    s = JSON.stringify(x)
    expect(s).to.equal('{"foo":"a","bar":5}')
    r = st.fromJSON(JSON.parse s)
    expect("#{r}").to.equal('{foo: "a", bar: 5}')

    # also, expect it not to crash
    expect(st.Type(x)).to.equal(x)

    expect(-> (st.Type)({foo:"a", bar:5})).to.throw()

    expect(-> new st()).to.throw()
    expect(-> new st({})).to.throw()
    expect(-> new st({foo: 5})).to.throw()
    expect(-> new st({bar: "x"})).to.throw()
    expect(-> new st({foo: "x", bar: "y"})).to.throw()
    expect(-> new st({foo: "x", bar: 5, baz: 17})).to.throw()
    x.bar = "x"
    expect("#{x}").to.equal('{foo: "a", bar: "x"}')
    expect(-> JSON.stringify(x)).to.throw()
    expect(-> st.Type(x)).to.throw()

  it "fromJSON rejects objects and primitives when an array is expected", ->
    St = struct.define('St', [
      ['a', T.arrayOf T.uint]
    ])
    expect(-> St.fromJSON {a: {}, baz: 5}).to.throw /Wanted arrayOf\(T\.uint\), but fromJSON input is not an array: {}/
    expect(-> St.fromJSON {a: 5, baz: 5}).to.throw /Wanted arrayOf\(T\.uint\), but fromJSON input is not an array: 5/

  it "ignores unknown fields in fromJSON", ->
    St = struct.define('St', [
      ['foo', T.string],
      ['bar', T.int],
    ])
    d = St.fromJSON({"foo": "a", "bar": 5, "baz": 17})
    expect("#{d}").to.equal('{foo: "a", bar: 5}')

  describe "with nested structs", ->
    beforeEach ->
      @St1 = struct.define('St1', [
        ['foo', T.string],
        ['bar', T.int],
      ])
      @St2 = struct.define('St2', [
        ['a', @St1],
        ['baz', T.int],
      ])

      @St1a = struct.define('St1a', [
        ['bar', T.int],
        ['foo', T.string],
      ])
      @St2a = struct.define('St2a', [
        ['a', @St1a],
        ['baz', T.int],
      ])

    it "ignores unknown fields in fromJSON", ->
      d = @St2.fromJSON {a: {"foo": "a", "bar": 5, "baz": 17}, baz: 5}
      expect("#{d}").to.equal '{a: {foo: "a", bar: 5}, baz: 5}'

    it "respects order of fields in toString", ->
      d = @St2.fromJSON {a: {"foo": "a", "bar": 5}, baz: 5}
      expect("#{d}").to.equal '{a: {foo: "a", bar: 5}, baz: 5}'
      d = @St2a.fromJSON {a: {"foo": "a", "bar": 5}, baz: 5}
      expect("#{d}").to.equal '{a: {bar: 5, foo: "a"}, baz: 5}'

  describe "with nullable/arrayOf nested structs", ->
    beforeEach ->
      @St1 = struct.define('St1', [
        ['foo', T.string],
      ])

    it "ignores unknown fields in fromJSON for nullable", ->
      St2 = struct.define('St2', [
        ['a', T.nullable @St1]
      ])
      d = St2.fromJSON {a: {"foo": "a", "baz": 17}, baz: 5}
      expect(JSON.stringify d).to.equal '{"a":{"foo":"a"}}'

    it "ignores unknown fields in fromJSON for arrayOf", ->
      St2 = struct.define('St2', [
        ['a', T.arrayOf @St1]
      ])
      d = St2.fromJSON {a: [{"foo": "a", "baz": 17}], baz: 5}
      expect(JSON.stringify d).to.equal '{"a":[{"foo":"a"}]}'

    it "ignores unknown fields in fromJSON for nullable arrayOf", ->
      St2 = struct.define('St2', [
        ['a', T.nullable T.arrayOf @St1]
      ])
      d = St2.fromJSON {a: [{"foo": "a", "baz": 17}], baz: 5}
      expect(JSON.stringify d).to.equal '{"a":[{"foo":"a"}]}'

    it "ignores unknown fields in fromJSON for arrayOf nullable", ->
      St2 = struct.define('St2', [
        ['a', T.arrayOf T.nullable @St1]
      ])
      d = St2.fromJSON {a: [{"foo": "a", "baz": 17}], baz: 5}
      expect(JSON.stringify d).to.equal '{"a":[{"foo":"a"}]}'


  it "sets up a proper object with constructor, prototype etc.", ->
    st = struct.define('st', [])
    x = new st {}
    expect(x).to.be.instanceOf st
    expect(x.constructor).to.equal st
    expect(x.__proto__).to.equal st.prototype
    for own k, v of x
      assert false, "unexpected own property: #{k}=#{JSON.stringify v}"
    expected_inherited = ['toJSON', 'toString']
    actual_inherited = []
    for k, v of x
      actual_inherited.push k
    actual_inherited.sort()
    expect(actual_inherited).to.deep.equal expected_inherited


  it "struct serializes and deserializes", ->
    inner = struct.define('inner', [
      ['foo', T.string],
      ['bar', T.int],
    ])
    outer = struct.define('outer', [
      ['x', inner],
    ])
    init = {x: {foo: 'x', bar: 4}}
    x = new outer(init)
    expect(JSON.parse(JSON.stringify(x))).to.deep.equal(init)
    inner.Type(x.x)
    outer.Type(x)

    s = JSON.stringify(x)
    y = outer.fromJSON(JSON.parse s)
    expect(y).to.deep.equal(x)
    inner.Type(y.x)
    outer.Type(x)

  it "struct supports default values", ->
    initInt = 9
    st = struct.define('st', [
      ['foo', T.string, {init:"bar"}],
      ['baz', T.int, {initFn: -> initInt}],
    ])
    expect(struct.toJSO(new st({}))).to.deep.equal({foo: "bar", baz: 9})
    initInt = 4
    expect(struct.toJSO(new st({}))).to.deep.equal({foo: "bar", baz: 4})
    expect(struct.toJSO(new st({foo: '3'}))).to.deep.equal({foo: "3", baz: 4})
    expect(struct.toJSO(new st({baz: 2}))).to.deep.equal({foo: "bar", baz: 2})

    # test deserializing
    expect(struct.toJSO(st.fromJSON({}))).to.deep.equal({foo: "bar", baz: 4})

  it "can handle nullable and arrayOf", ->
    inner = struct.define('inner', [
      ['a', T.string]
      ['b', T.string, {init: 'x'}]
    ])
    outer = struct.define('outer', [
      ['x', T.nullable(inner)],
      ['y', T.arrayOf(inner)],
    ])
    expect(struct.toJSO(new outer({x: {a: '5'}, y: [{a: '6', b: '7'}]})))
      .to.deep.equal({x: {a: '5', b: 'x'}, y: [{a: '6', b: '7'}]})
    expect(struct.toJSO(outer.fromJSON({"x": {"a": "5"}, "y": [{"a": "6", "b": "7"}]})))
      .to.deep.equal({x: {a: '5', b: 'x'}, y: [{a: '6', b: '7'}]})


describe 'union_as_list', ->

  it "works somewhat", ->
    Maybe = struct.union_as_list 'Maybe', [
      ['Some', [['value', T.uint]]]
      ['None', []]
    ]
    init = ['Some', 4]
    x = Maybe.from_array init
    expect("#{x}").to.deep.equal('Maybe.Some(["Some",4])')
    expect(JSON.parse(JSON.stringify(x))).to.deep.equal(init)
    Maybe.Type x
    Maybe.Some.Type x

  it "sets up a proper object with constructor, prototype etc.", ->
    Unit = struct.union_as_list 'Unit', [
      ['Nil', []]
    ]
    x = Unit.from_array ['Nil']
    expect(x).to.be.instanceOf Unit.Nil
    expect(x.constructor).to.equal Unit.Nil
    expect(x.__proto__).to.equal Unit.Nil.prototype
    expected_own = ['_tag']
    actual_own = []
    for own k, v of x
      actual_own.push k
    actual_own.sort()
    expect(actual_own).to.deep.equal expected_own

    expected_all = ['_tag', 'tag', 'toJSON', 'toString']
    actual_all = []
    for k, v of x
      actual_all.push k
    actual_all.sort()
    expect(actual_all).to.deep.equal expected_all

  it "ignores unknown fields in fromJSON", ->
    Unit = struct.union_as_list 'Unit', [
      ['Nil', []]
    ]
    x = Unit.fromJSON ['Nil', 5, 13]
    expect(JSON.stringify x).to.equal '["Nil"]'

  it "parses when nested in struct", ->
    Unit = struct.union_as_list 'Unit', [
      ['Nil', []]
    ]
    St = struct.define 'St', [
      ['x', Unit]
    ]
    x = St.fromJSON {x: ['Nil']}
    expect(x.x).to.be.instanceOf Unit.Nil
    expect(x.x.constructor).to.equal Unit.Nil
    expect(x.x.__proto__).to.equal Unit.Nil.prototype
    expect(JSON.stringify x).to.equal '{"x":["Nil"]}'

  it "parses when nested in struct, ignoring unknown fields", ->
    Unit = struct.union_as_list 'Unit', [
      ['Nil', []]
    ]
    St = struct.define 'St', [
      ['x', Unit]
    ]
    x = St.fromJSON {x: ['Nil', 5, 13]}
    expect(x.x).to.be.instanceOf Unit.Nil
    expect(x.x.constructor).to.equal Unit.Nil
    expect(x.x.__proto__).to.equal Unit.Nil.prototype
    expect(JSON.stringify x).to.equal '{"x":["Nil"]}'

