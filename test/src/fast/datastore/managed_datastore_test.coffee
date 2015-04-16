merge_test_util_m = require '../../datastore/merge_test_util'

impl = Dropbox.Datastore.impl
{DefaultResolver, DatastoreModel, Change, FieldOp, LocalDelta} = impl


class OpGenerator
  copy_value = (v) ->
    return (if v instanceof Array then v.slice() else v)
  field_op_from_value = (v) ->
    val = copy_value v
    arr = if val? then ['P', val] else ['D']
    return (FieldOp.from_array arr)

  constructor: (settings) ->
    {@MAX_OPS, @RID_POOL, @RFIELD_POOL, @LIST_VALUE_POOL} = settings

  get_list_field_ops: (length, possible_values) ->
    ret = []
    for i in [0...length]
      for val in possible_values
        ret.push ['LP', i, val]
    for i in [0...(length + 1)]
      for val in possible_values
        ret.push ['LI', i, val]
    for i in [0...length]
      ret.push ['LD', i]
    for i in [0...length]
      for j in [0...length]
        ret.push ['LM', i, j]
    return (FieldOp.from_array arr for arr in ret)

  get_updates: (record) ->
    field_names = []
    possible_field_ops = {}
    for field, values of @RFIELD_POOL
      field_names.push field
      possible_field_ops[field] = []

    for field, values of @RFIELD_POOL
      for v in values
        if v instanceof Array and v.length == 0 and field not of record
          possible_field_ops[field].push (FieldOp.from_array ['LC'])
        possible_field_ops[field].push (field_op_from_value v)
      if record[field] instanceof Array
        for field_op in (@get_list_field_ops record[field].length, @LIST_VALUE_POOL)
          possible_field_ops[field].push field_op

    ret = []
    get_all_pairs = (field_name1, field_name2) ->
      field_ops1 = possible_field_ops[field_name1]
      field_ops2 = possible_field_ops[field_name2]
      for op1 in field_ops1
        for op2 in field_ops2
          updates = {}
          updates[field_name1] = op1
          updates[field_name2] = op2
          ret.push updates

    for field, ops of possible_field_ops
      for op in ops
        updates = {}
        updates[field] = op
        ret.push updates

    # uncomment this to test resolving multiple field ops in one update,
    # doing this will slow down the test
    for i in [0...field_names.length]
      for j in [0...i]
        get_all_pairs field_names[i], field_names[j]
    return ret

  get_possible_ops: (holder) ->
    data = holder.data
    all_ops = []
    for tid, table of data
      for rid in @RID_POOL
        continue if rid of table
        for field, values of @RFIELD_POOL
          for v in values
            continue unless v?
            ins_fields = {}
            ins_fields[field] = (copy_value v)
            all_ops.push (Change.from_array ['I', tid, rid, ins_fields])

      for rid, record of table
        for updates in (@get_updates record)
          chg = (Change.from_array ['U', tid, rid, updates])
          chg.undo_extra = {}
          for k of updates
            chg.undo_extra[k] = JSON.parse (JSON.stringify (record[k] ? null))
          all_ops.push chg
        all_ops.push (Change.from_array ['D', tid, rid])

    # don't search whole space to make test run faster
    if not @MAX_OPS? or all_ops.length <= @MAX_OPS
      return all_ops

    # randomly select @MAX_OPS of the ops
    num_left = @MAX_OPS
    filtered_ops = []
    for op, idx in all_ops
      if Math.random() < (num_left / (all_ops.length - idx))
        filtered_ops.push op
        num_left--
    return filtered_ops

initial_data = {
  't': {
    'r0': {'f0': 'hello', 'f1': ['x', 3]}
    'r1': {'f0': 101}
    }
  }

FAST_SETTINGS =
  X_DEPTH: 1
  Y_DEPTH: 1
  RID_POOL: ['r0', 'r1']
  RFIELD_POOL:
    'f0': [null, 1, {I: '2'}]
    'f1': ['a', [2]]
  LIST_VALUE_POOL: ['b', 3]
  MAX_OPS: 10

THOROUGH_SETTINGS =
  X_DEPTH: 2
  Y_DEPTH: 2
  RID_POOL: ['r0', 'r1']
  RFIELD_POOL:
    'f0': [null, 1, {I: '2'}, 2]
    'f1': ['a', [3, 4]]
    'f2': [null, 'b', []]
  LIST_VALUE_POOL: ['c', 5]
  MAX_OPS: 15
  REPORT_EVERY: 100
  TIMEOUT: 60 * 1000

apply_op = (holder, op) ->
  dm = new DatastoreModel false, holder.data
  dm.apply_change false, op
  holder.data = dm.raw_data()
  undefined

resolver = new DefaultResolver
resolve = (resolver._transform_list.bind resolver)

do_test = (settings, resolve_fn, done) ->
  num_ops = {x: settings.X_DEPTH, y: settings.Y_DEPTH}
  opts = {report_every: (settings.REPORT_EVERY ? 100000000)} # TODO

  op_gen = new OpGenerator settings
  mt = merge_test_util_m.make_merge_tester num_ops, {data: (impl.clone initial_data)},
    apply_op, resolve_fn, (op_gen.get_possible_ops.bind op_gen), opts

  # TODO: 30 is just the depth limit but we'll never hit it as long as
  # num_ops.x + num_ops.y < 30
  mt.run 30, (err, tested) ->
    # console.log tested
    throw err if err
    done()

describe 'DefaultResolver', ->
  it 'test basic resolution', (done) ->
    settings = FAST_SETTINGS
    @timeout settings.TIMEOUT if settings.TIMEOUT?
    do_test settings, resolve, done

  it 'test sum resolution doesn\'t throw errors', (done) ->
    settings = FAST_SETTINGS
    @timeout settings.TIMEOUT if settings.TIMEOUT?

    sum_resolver = new DefaultResolver
    sum_resolver.add_resolution_rule 't', 'f0', 'sum'
    sum_resolve = (sum_resolver._transform_list.bind sum_resolver)
    do_test settings, sum_resolve, done

  it 'test max resolution doesn\'t throw errors', (done) ->
    settings = FAST_SETTINGS
    @timeout settings.TIMEOUT if settings.TIMEOUT?

    max_resolver = new DefaultResolver
    max_resolver.add_resolution_rule 't', 'f0', 'max'
    max_resolve = (max_resolver._transform_list.bind max_resolver)
    do_test settings, max_resolve, done


apply_change = (model, enforce_limits, json) ->
  model.apply_change enforce_limits, impl.Change.from_array json

describe 'DatastoreModel', ->

  beforeEach ->

  it 'clones arrays passed into P', ->
    tid = "application_state"
    rid = "_04e08b3fa2e271_js_-8kCJUOTfd5pw"

    array = []
    model = new impl.DatastoreModel true, {}
    apply_change model, true, ['I', tid, rid, {}]
    apply_change model, true, ['U', tid, rid, { "active_strokes": ["P", array] }]
    apply_change model, true, ['U', tid, rid, {
      "active_strokes": ["LI", 0, "_04e08e02664bd8_js_QdMMAxdCnYw2C"]
    }]
    expect(array).to.deep.equal([])

  it 'tracks sizes', ->
    model = new impl.DatastoreModel true, {}
    expect(model.size()).to.equal 1000
    apply_change model, true, ['I', 't1', 'r1', {}]
    expect(model.size()).to.equal 1100
    apply_change model, true, ['I', 't2', 'r2', {}]
    expect(model.size()).to.equal 1200
    apply_change model, true, ['U', 't2', 'r2', {'f': ['P', 3]}]
    expect(model.size()).to.equal 1300
    apply_change model, true, ['U', 't2', 'r2', {'f': ['P', "a"]}]
    expect(model.size()).to.equal 1301
    apply_change model, true, ['U', 't2', 'r2', {'f': ['P', "abc"]}]
    expect(model.size()).to.equal 1303
    apply_change model, true, ['I', 't1', 'r3', {'f': "abc"}]
    expect(model.size()).to.equal 1506
    apply_change model, true, ['U', 't2', 'r2', {'f': ['D']}]
    expect(model.size()).to.equal 1403
    apply_change model, true, ['D', 't2', 'r2']
    expect(model.size()).to.equal 1303

  it 'tracks record counts', ->
    model = new impl.DatastoreModel true, {}
    expect(model.record_count()).to.equal 0
    apply_change model, true, ['I', 't1', 'r1', {}]
    expect(model.record_count()).to.equal 1
    apply_change model, true, ['I', 't2', 'r2', {}]
    expect(model.record_count()).to.equal 2
    apply_change model, true, ['D', 't2', 'r2']
    expect(model.record_count()).to.equal 1

  it 'rejects large records', ->
    model = new impl.DatastoreModel true, {}
    expect(model.size()).to.equal 1000
    expect(=>
      # 200 is the base size of a record with one field.
      apply_change model, true, ['I', 't1', 'r1', {"f": ("x" for i in [0...102400-199]).join("")}]
    ).to.throw /Record \(t1, r1\) too large: 102401 bytes/
    expect(model.size()).to.equal 1000
    # 200 is the base size of a record with one field.
    apply_change model, true, ['I', 't1', 'r1', {"f": ("x" for i in [0...102400-1200]).join("")}]
    expect(model.size()).to.equal 102400

  it 'rejects large datastores', ->
    # 200 is the base size of a record with one field.
    one_k_record_data = {'f': ("x" for i in [0...1024-200]).join("")}
    model = new impl.DatastoreModel true, {}
    expect(model.size()).to.equal 1000
    for i in [0...1024 * 10 - 1]
      expect(model.size()).to.equal (i * 1024 + 1000)
      apply_change model, true, ['I', 't1', "r#{i}", one_k_record_data]
    expect(model.size()).to.equal 10485736 # (+ (* (1- 10240) 1024) 1000)
    expect(=>
      apply_change model, true, ['I', 't1', "s", one_k_record_data]
    ).to.throw /Datastore too large: 10486760 bytes/ # (+ (* 10 1024 1024) 1000)
    expect(model.size()).to.equal 10485736
    apply_change model, false, ['I', 't1', "s", one_k_record_data]
    expect(model.size()).to.equal 10486760


describe 'value_size', ->
  it 'works', ->
    # adapted from SizeComputationTestCase.test_get_value_size in the
    # server code base
    expect(impl.value_size true).to.equal 0
    expect(impl.value_size 3.14).to.equal 0
    expect(impl.value_size 2).to.equal 0
    expect(impl.value_size {'T': '23'}).to.equal 0
    expect(impl.value_size {'I': '23'}).to.equal 0
    expect(impl.value_size {'I': '5'}).to.equal 0
    expect(impl.value_size {'N': 'nan'}).to.equal 0
    expect(impl.value_size {'N': '+inf'}).to.equal 0
    expect(impl.value_size {'N': '-inf'}).to.equal 0
    expect(impl.value_size '').to.equal 0
    expect(impl.value_size 'abc').to.equal 3
    expect(impl.value_size 'unicode').to.equal 7
    # node and coffeescript don't understand this ECMAScript 6 syntax yet;
    # but countUtf8Bytes is tested elsewhere anyway
    #expect(impl.value_size '\xff\u1234\u{12345}').to.equal 9
    #expect(impl.value_size eval('"\\xff\\u1234\\u{12345}"')).to.equal 9
    expect(impl.value_size {'B': ''}).to.equal 0
    expect(impl.value_size {'B': 'a'}).to.equal 1
    expect(impl.value_size {'B': 'aa'}).to.equal 2
    expect(impl.value_size {'B': 'aaa'}).to.equal 3
    expect(impl.value_size {'B': 'aaaa'}).to.equal 3
    expect(impl.value_size [2, true, {'T': '23'}]).to.equal 60
    expect(impl.value_size ['abc', {'B': 'aa'}, 3]).to.equal 65
    expect(-> impl.value_size {}).to.throw /Unexpected object: {}/
    expect(-> impl.value_size {'D': 4}).to.throw /Unexpected object: {"D":4}/


describe 'LocalDelta', ->
  describe '_is_simple_mtime_update', ->

    accept = (c) ->
      expect(LocalDelta._is_simple_mtime_update c).to.be.true

    reject = (c) ->
      expect(LocalDelta._is_simple_mtime_update c).to.be.false

    it 'accepts simple mtime updates', ->
      accept Change.from_array ['U', ':info', 'info', { mtime: ['P', 12] }]
      accept Change.from_array ['U', ':info', 'info', { mtime: ['P', 'foo'] }]

    it 'rejects other records', ->
      reject Change.from_array ['U', ':info1', 'info', { mtime: ['P', 12] }]
      reject Change.from_array ['U', ':info', 'info1', { mtime: ['P', 'foo'] }]
      reject Change.from_array ['U', 'a', 'b', { mtime: ['P', 'foo'] }]

    it 'rejects other fields', ->
      reject Change.from_array ['U', ':info', 'info', { mtime: ['P', 'foo'], other: ['P', 'x'] }]
      reject Change.from_array ['U', ':info', 'info', { mtime: ['P', 'foo'], other: ['D'] }]
      reject Change.from_array ['U', ':info', 'info', { other: ['P', 'x'] }]

    it 'rejects other field ops', ->
      reject Change.from_array ['U', ':info', 'info', { mtime: ['D'] }]
      reject Change.from_array ['U', ':info', 'info', { mtime: ['LC'] }]

    it 'rejects other ops on the info record', ->
      reject Change.from_array ['I', ':info', 'info', { mtime: "foo" }]
      reject Change.from_array ['D', ':info', 'info']


  describe '_affects_mtime', ->

    accept = (c) ->
      expect(LocalDelta._affects_mtime c).to.be.true

    reject = (c) ->
      expect(LocalDelta._affects_mtime c).to.be.false

    it 'accepts changes to mtime field', ->
      accept Change.from_array ['U', ':info', 'info', { mtime: ['P', 12] }]
      accept Change.from_array ['U', ':info', 'info', { mtime: ['P', 'foo'] }]
      accept Change.from_array ['U', ':info', 'info', { mtime: ['D'] }]
      accept Change.from_array ['U', ':info', 'info', { mtime: ['LC'] }]

    it 'accepts changes to other fields in addition to mtime field', ->
      accept Change.from_array ['U', ':info', 'info', { mtime: ['P', 12], title: ['D'] }]

    it 'accepts record deletion', ->
      accept Change.from_array ['D', ':info', 'info']

    it 'accepts record insertion', ->
      accept Change.from_array ['I', ':info', 'info', {}]
      accept Change.from_array ['I', ':info', 'info', { mtime: ['P', 'foo'] }]

    it 'rejects info record updates that don\'t affect mtime', ->
      reject Change.from_array ['U', ':info', 'info', { title: ['P', 'foo'] }]
      reject Change.from_array ['U', ':info', 'info', { title: ['P', 'foo'], other: ['D'] }]
      reject Change.from_array ['U', ':info', 'info', {}]

    it 'rejects insertion of other records', ->
      reject Change.from_array ['I', ':info', 'info1', { mtime: ['P', 'foo']}]
      reject Change.from_array ['I', ':info1', 'info', { mtime: ['P', 'foo']}]
      reject Change.from_array ['I', 'a', 'b', { mtime: ['P', 'foo']}]

    it 'rejects deletion of other records', ->
      reject Change.from_array ['D', ':info', 'info1']
      reject Change.from_array ['D', ':info1', 'info']
      reject Change.from_array ['D', 'a', 'b']


  describe 'LocalDelta\'s basic delta compression for mtime updates', ->

    it 'compresses simple mtime updates', ->
      d = new LocalDelta [], []
      d.add_change (Change.from_array ['I', 'a', 'b', {foo: "bar"}]), null
      d.add_change (Change.from_array ['U', ':info', 'info', {mtime: ["P", 2]}]), null
      expect(d.changes.length).to.equal 2
      d.add_change (Change.from_array ['U', ':info', 'info', {mtime: ["P", 3]}]), null
      expect(d.changes.length).to.equal 2
      d.add_change (Change.from_array ['D', ':info', 'info']), null
      d.add_change (Change.from_array ['I', ':info', 'info', {mtime: 4}]), null
      d.add_change (Change.from_array ['U', ':info', 'info', {mtime: ["P", 5]}]), null
      expect(d.changes.length).to.equal 5
      d.add_change (Change.from_array ['U', ':info', 'info', {mtime: ["P", 6]}]), null
      expect(d.changes.length).to.equal 5
      d.add_change (Change.from_array ['U', ':info', 'info', {title: ["P", "t2"]}]), null
      expect(d.changes.length).to.equal 6
      d.add_change (Change.from_array ['U', ':info', 'info', {mtime: ["P", 7]}]), null
      expect(d.changes.length).to.equal 6
      d.add_change (Change.from_array ['U', ':info', 'info', {title: ["P", "t2"], mtime: ["P", 8]}]), null
      expect(d.changes.length).to.equal 7
      d.add_change (Change.from_array ['U', ':info', 'info', {mtime: ["P", 9]}]), null
      expect(d.changes.length).to.equal 8

      expect(JSON.parse JSON.stringify d.changes).to.deep.equal [
        ['I', 'a', 'b', {foo: "bar"}],
        ['U', ':info', 'info', {mtime: ["P", 3]}],
        ['D', ':info', 'info'],
        ['I', ':info', 'info', {mtime: 4}],
        ['U', ':info', 'info', {title: ["P", "t2"]}],
        ['U', ':info', 'info', {mtime: ["P", 7]}],
        ['U', ':info', 'info', {title: ["P", "t2"], mtime: ["P", 8]}],
        ['U', ':info', 'info', {mtime: ["P", 9]}],
      ]
