
class Cp1Resolver
  constructor: (@transform) ->

  transform_list: (local_ops, server_ops) ->
    if local_ops.length == 0
      return [[], server_ops]
    if server_ops.length == 0
      return [local_ops, []]

    # TODO: can make this faster probably, and should also make
    # iterative to avoid stack problems.
    first_local = local_ops[0]
    first_server = server_ops[0]
    [new_local, new_server] = @transform first_local, first_server

    [local_rest, new_server] = @transform_list (local_ops.slice 1), new_server

    (new_local.push op) for op in local_rest
    [new_local, server_rest] = @transform_list new_local, (server_ops.slice 1)

    (new_server.push op) for op in server_rest
    return [new_local, new_server]

  resolve: (local_op_batches, server_ops) ->
    ops_to_apply = server_ops.slice()
    new_local_batches = []

    for op_batch in local_op_batches
      [local_batch, ops_to_apply] = @transform_list op_batch, ops_to_apply
      new_local_batches.push local_batch

    return [new_local_batches, ops_to_apply]

exports.Cp1Resolver = Cp1Resolver