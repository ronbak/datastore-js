assert = require 'assert'
merge_test_util_m = require '../../datastore/merge_test_util'
resolver_util_m = require '../../datastore/resolver_util'

FieldOpTransformer = Dropbox.Datastore.impl.FieldOpTransformer
FieldOp = Dropbox.Datastore.impl.FieldOp

num_ops =
  x: 1
  y: 1

initial_data = [1, 2, 3]

apply_op = (data, op) ->
  switch op.tag()
    when 'LP'
      assert (0 <= op.at < data.length)
      data[op.at] = op.value
    when 'LI'
      assert (0 <= op.before <= data.length)
      data.splice op.before, 0, op.value
    when 'LD'
      assert (0 <= op.at < data.length)
      data.splice op.at, 1
    when 'LM'
      assert (0 <= op.from < data.length)
      value = data[op.from]
      data.splice op.from, 1
      assert (0 <= op.to <= data.length)
      data.splice op.to, 0, value
    else throw new Error "unrecognized tag #{op.tag()}"

# TODO: test the various custom transformers
field_op_transformer = new FieldOpTransformer
transform = (op1, op2) ->
  [new_op1, new_op2] = field_op_transformer.transform op1, op2
  return [(if new_op1? then [new_op1] else []),
          (if new_op2? then [new_op2] else [])]
resolver = new resolver_util_m.Cp1Resolver transform
resolve = (resolver.transform_list.bind resolver)

get_possible_ops = (data) ->
  POSSIBLE_VALUES = ['a', 'b']
  ret = []

  for i in [0...data.length]
    for val in POSSIBLE_VALUES
      ret.push ['LP', i, val]
  for i in [0...(data.length + 1)]
    for val in POSSIBLE_VALUES
      ret.push ['LI', i, val]
  for i in [0...data.length]
    ret.push ['LD', i]
  for i in [0...data.length]
    for j in [0...data.length]
      ret.push ['LM', i, j]

  return (FieldOp.from_array arr for arr in ret)

describe 'Dropbox.Datastore.FieldOpTransformer', ->
  it 'list transforms are consistent', (done) ->
    this.timeout 4000 # this should be way faster, but test server seems to run it slowly

    opts =
      report_every: 10000000 # large number for now so that nothing is reported
    mt = merge_test_util_m.make_merge_tester num_ops, initial_data,
      apply_op, resolve, get_possible_ops, opts

    mt.run 30, (err, tested) =>
      # console.log tested
      throw err if err
      done()
