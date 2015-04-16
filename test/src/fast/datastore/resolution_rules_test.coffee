impl = Dropbox.Datastore.impl
DefaultResolver = impl.DefaultResolver
DatastoreModel = impl.DatastoreModel
Change = impl.Change
FieldOp = impl.FieldOp
FieldOpTransformer = impl.FieldOpTransformer

initial_data = {
  't': {
    'r': {}
  }
}

apply_change = (holder, change) ->
  dm = new DatastoreModel false, holder.data
  dm.apply_change false, change
  holder.data = dm.raw_data()
  undefined

val_to_field_op = (val) ->
  if val?
    return FieldOp.from_array ['P', val]
  return FieldOp.from_array ['D']

describe 'DefaultResolver', ->
  beforeEach ->
    @resolver = new DefaultResolver

    @make_change = (val) ->
      return Change.from_array ['U', 't', 'r', {'f': (val_to_field_op val)}]

    @do_resolution_test_case = (test_case) ->
      local = ((@make_change v) for v in test_case.local)
      local[0].undo_extra = {'f': null}
      for i in [0...(local.length - 1)]
        local[i + 1].undo_extra = {'f': test_case.local[i]}

      remote = ((@make_change v) for v in test_case.remote)

      [new_local, new_remote] = @resolver._transform_list local, remote

      holder = {data: impl.clone initial_data}
      for chg in (remote.concat new_local)
        apply_change holder, chg

      (expect holder.data.t.r.f).to.eql test_case.result

  describe 'with sum rule', ->
    beforeEach ->
      @resolver.add_resolution_rule 't', 'f', 'sum'

    it 'computes sums correctly', ->
      TEST_CASES = [
        {old: null, v1: 2, v2: 4, result: 6}
        ]

      do_test_case = (test_case) ->
        old = test_case.old ? {I: '0'}
        result = FieldOpTransformer._compute_sum old, test_case.v1, test_case.v2
        (expect result).to.equal test_case.result

      for test_case in TEST_CASES
        do_test_case test_case

    it 'resolves sums correctly', ->
      TEST_CASES = [
        {local: [2], remote: [-3], result: -1}
        {local: [{I: '2'}], remote: [{I: '-3'}], result: {I: '-1'}}
        {local: [2, 7, -13], remote: [-3], result: -16}
        {local: [2], remote: [-3, 7], result: 9}
        {local: [2, 7, -13], remote: [-3, 7], result: -6}
        {local: [2.5, {I: '54'}], remote: [{I: '-3'}, 1.5, 3], result: 57}

        {local: [2, 'a', 3, 4], remote: [5, 6], result: 9}
        ]

      for test_case in TEST_CASES
        @do_resolution_test_case test_case

  describe 'with min/max rule', ->
    it 'computes comparisons correctly', ->
      lt = FieldOpTransformer._is_less_than
      COMPARE_TWO_CASES = [[1, 3, true],
                           ['abc', 'abcd', true],
                           [{B: 'ABCD'}, {B: 'ZXY'}, true],
                           [{T: '1234'}, {T: '987'}, false],
                           [[1, 3, 2], [1, 2, 3], false]]
      IN_ORDER_CASES = [[null, false, 0, {I: '1'}, 'a', {B: 'F'}, {T: '123456'}, []],
                        ['a', 'aa', 'ab', 'b', 'ba', 'bb'],
                        [{I: '1'}, 1.5, {I: '2'}],
                        [[1], [1, 1], [1, 2], [2], [2, 1], [2, 2]]]

      for [v1, v2, result] in COMPARE_TWO_CASES
        expect(lt v1, v2).to.equal result
        if result is true
          expect(lt v2, v1).to.equal false
      for list in IN_ORDER_CASES
        for i in [0...list.length]
          for j in [0...i]
            expect(lt list[j], list[i]).to.equal true

    it 'resolves min correctly', ->
      @resolver.add_resolution_rule 't', 'f', 'min'

      TEST_CASES = [
        {local: [2, 5, 1], remote: [3, {I: '-2'}], result: {I: '-2'}}
        {local: ['c', 'b', 'a'], remote: ['d', 'c'], result: 'a'}
        ]

      for test_case in TEST_CASES
        @do_resolution_test_case test_case

    it 'resolves max correctly', ->
      @resolver.add_resolution_rule 't', 'f', 'max'

      TEST_CASES = [
        {local: [2, {I: '4'}, 6], remote: [3, {I: '5'}, 7], result: 7}
        ]

      for test_case in TEST_CASES
        @do_resolution_test_case test_case
