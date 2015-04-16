# Some datastore-related type definitions and similar stuff.

impl.nonzero_int64_approximate_regex = new RegExp '^-?[1-9][0-9]{0,18}$'
impl.int64_max_str = '9223372036854775807'
impl.int64_min_str = '-9223372036854775808'

# Sharing constant definitions
impl.ACL_TID = ':acl'
impl.ROLE_OWNER = 3000
impl.ROLE_EDITOR = 2000
impl.ROLE_VIEWER = 1000

# @private
impl.int64_string_less_than = (x, y) ->
  return false if x == y
  x_neg = (x.charAt 0) == '0'
  y_neg = (y.charAt 0) == '0'
  return true if x_neg and not y_neg
  return false if y_neg and not x_neg
  x_magnitude_larger = if x.length == y.length
    x > y
  else
    (x.length > y.length)
  if x_neg and y_neg
    return x_magnitude_larger
  return not x_magnitude_larger

# @private
impl.is_valid_int64_string = (x) ->
  return false unless T.is_string x
  return true if x == '0'
  return false unless impl.nonzero_int64_approximate_regex.test x
  if (x.charAt 0) == '-'
     (x.length < impl.int64_min_str.length) or (x <= impl.int64_min_str)
  else
    return (x.length < impl.int64_max_str.length) or (x <= impl.int64_max_str)

# @private
impl.is_wrapped_atomic_field_value = (x) ->
  return false unless T.is_simple_map x
  keys = Object.keys x
  return false unless keys.length == 1
  switch keys[0]
    when 'B'
      # probably OK to let the server do further validation
      return T.is_string x.B
    when 'N'
      return x.N in ['nan', '+inf', '-inf']
    when 'I', 'T'
      y = x.I ? x.T
      return impl.is_valid_int64_string y
    else
      return false

# @private
impl.is_atomic_field_value = (x) ->
  return (T.is_bool x) or (T.is_json_number x) or (T.is_string x) or (impl.is_wrapped_atomic_field_value x)

# @private
impl.is_list_value = (x) ->
  if not T.is_array x
    return false
  else
    for elt in x
      if not impl.is_atomic_field_value elt
        return false
    return true

# @private
impl.is_compound_field_value = (x) ->
  return (impl.is_atomic_field_value x) or (impl.is_list_value x)

# @private
impl.atomic_field_value = (x, what, wanted, top) ->
  wanted ?= "atomic field value"
  T.check (impl.is_atomic_field_value x),
    "is not an atomic field value",
    x, what, wanted, top
  return x

# @private
impl.list_value = (x, what, wanted, top) ->
  wanted ?= "list value"
  (T.arrayOf impl.atomic_field_value) x, what, wanted, top
  return x

# @private
# atomic field value or list.
impl.compound_field_value = (x, what, wanted, top) ->
  wanted ?= "field value"
  if T.is_array x
    return impl.list_value x, what, wanted, top
  else
    return impl.atomic_field_value x, what, wanted, top

impl.FieldOp = FieldOp = struct.union_as_list 'FieldOp', [
  ['P', [['value', impl.compound_field_value]]]
  ['D', []]
  ['LC', []]
  ['LP', [['at', T.uint], ['value', impl.atomic_field_value]]]
  ['LI', [['before', T.uint], ['value', impl.atomic_field_value]]]
  ['LD', [['at', T.uint]]]
  ['LM', [['from', T.uint], ['to', T.uint]]]
]

impl.datadict = T.simple_typed_map 'datadict', T.field_name, impl.compound_field_value
impl.update_datadict = T.simple_typed_map 'update_datadict', T.field_name, FieldOp

impl.Change = Change = struct.union_as_list 'Change', [
  ['I', [['tid', T.tid], ['rowid', T.rowid], ['fields', impl.datadict]]]
  ['U', [['tid', T.tid], ['rowid', T.rowid], ['updates', impl.update_datadict]]]
  ['D', [['tid', T.tid], ['rowid', T.rowid]]]
]

impl.Delta = Delta = struct.define 'Delta', [
  ['rev', T.uint]
  ['changes', (T.arrayOf Change)]
  ['nonce', T.string]
]

# In all of the below, role will be set for successful (and non-empty)
# responses for shareable datastores only.

ListDatastoresResponseItem = struct.define 'ListDatastoresResponseItem', [
  ['dsid', T.string]
  ['handle', T.string]
  ['rev', T.uint]
  ['role', (T.nullable T.uint), {init: null}]
  # The expected fields in here are title (with string value) and
  # mtime (with int64 value) but we shouldn't crash on other types.
  ['info', (T.nullable impl.datadict), {init: null}]
]

impl.ListDatastoresResponse = ListDatastoresResponse = struct.define 'ListDatastoresResponse', [
  ['token', T.string]
  ['datastores', T.arrayOf ListDatastoresResponseItem]
]

GetSnapshotResponseRow = struct.define 'GetSnapshotResponseRow', [
  ['tid', T.string]
  ['rowid', T.string]
  ['data', impl.datadict]
]

GetSnapshotResponse = struct.define 'GetSnapshotResponse', [
  ['rev', T.uint]
  ['role', (T.nullable T.uint), {init: null}]
  ['rows', T.arrayOf GetSnapshotResponseRow]
]

CreateDatastoreResponse = struct.define 'CreateDatastoreResponse', [
  ['handle', T.string]
  ['rev', T.uint]
  ['created', T.bool]
  ['role', (T.nullable T.uint), {init: null}]
]

GetDatastoreResponse = struct.define 'GetDatastoreResponse', [
  # Either handle and rev or notfound will be set.
  ['handle', (T.nullable T.string), {init: null}]
  ['rev', (T.nullable T.uint), {init: null}]
  ['role', (T.nullable T.uint), {init: null}]
  ['notfound', (T.nullable T.string), {init: null}]
]

DeleteDatastoreResponse = struct.define 'DeleteDatastoresResponse', [
  ['ok', T.string]
]

PutDeltaResponse = struct.define 'PutDeltaResponse', [
  # Exactly one of (rev, conflict, notfound, access_denied) will be set.
  ['rev', (T.nullable T.uint), {init: null}]
  ['role', (T.nullable T.uint), {init: null}]
  ['conflict', (T.nullable T.string), {init: null}]
  ['notfound', (T.nullable T.string), {init: null}]
  ['access_denied', (T.nullable T.string), {init: null}]
]

GetDeltasResponse = struct.define 'GetDeltasResponse', [
  ['deltas', (T.nullable T.arrayOf Delta), {init: null}]
  ['role', (T.nullable T.uint), {init: null}]
  ['notfound', (T.nullable T.string), {init: null}]
]

AwaitResponseDeltas = struct.define 'AwaitResponseDeltas', [
  ['deltas', (T.simple_typed_map 'deltas map', T.string, GetDeltasResponse)]
]

impl.AwaitResponse = AwaitResponse = struct.define 'AwaitResponse', [
  ['get_deltas', (T.nullable AwaitResponseDeltas), {init: null}]
  ['list_datastores', (T.nullable ListDatastoresResponse), {init: null}]
]
