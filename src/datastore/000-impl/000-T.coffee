# T is for Type.  This file defines utility functions like T.uint,
# T.int, T.string, etc. that can be used for concise type assertions:
#
# T.uint rev
#   may throw "Wanted uint, but is not an integer: 5.1"
#
# T.uint rev, "rev"
#   may throw "Wanted uint, but rev is not an integer: 5.1"
#
# T.uint rev, "rev", "revision number"
#   may throw "Wanted revision number, but rev is not an integer: 5.1"


# HACK: impl should probably in a different file
#
# "impl" contains private stuff that needs to be exposed for tests.
impl = Dropbox.Datastore.impl = {}

T = Dropbox.Datastore.impl.T = {}

# stringlike means a string, or a zero-arg function returning a
# string, or an object to be stringified with JSON.stringify().
#
# c: boolean, true if the object satisfies the condition
# m: a stringlike describing the negated condition (e.g., "is not a string")
# x: the object being tested (e.g., null)
# what: a stringlike describing where the object came from (e.g., "element 2")
# wanted: the top-level constraint we are checking (e.g., "array of strings")
# top: a stringlike containing the top-level object we are checking (e.g., "['a', 'b', null]")
#
# From that, we can generate a message: "Wanted: array of strings, but
# element 2 of array ['a', 'b', null] is not a string: null"]
#
# Not sure if this is good, but it is the first thing that came to my mind.
#
#
# All of these type-checking functions return their arg x to allow
# self-verifying documentation like "return
# T.int(some_other_function())" or "return cb null, T.uid(id)".


# TODO(dropbox): simplify all this somehow, not all messages fit in this
# pattern, so they end up looking confusing.


# @private
T.identity = (x) -> x

# @private
#
# We support different kinds of type specifiers:
# (1) T-style 1-to-4-arg type assertion functions as defined in this file
# (2) structs defined using struct.iced
# [(3) return values of nullable and arrayOf]
#
# It's somewhat messy that both are functions, but in (1), the
# function is a type assertion, while in (2), the function is a
# constructor.  As a hack, we use these helper functions to paper over
# the difference.  Essentially, a type specifier is something that
# works when passed as an argument to these two functions.
T.get_coerce_fn = (type) ->
  return type.coerce if type.coerce?
  # TODO: this is a bit hacked in for compatibility w/ objects that
  # support dump_json/load_json -- eventually we need to decide which
  # subset of {coerce, load_json, dump_json, fromJSON, toJSON} (or
  # maybe some other interface entirely) we will need classes to
  # support in order to be serializable within structs
  if type.load_json?
    return (x) -> if (x instanceof type) then x else type.load_json x
  return T.identity
# @private
T.get_T_fn = (type) ->
  if type.Type? then type.Type else type


# @private
T.str = (x) ->
  if T.is_string x
    x
  else if T.is_function x
    x()
  else
    JSON.stringify x

# @private
#
# Use (T.assert foo, -> "foo is falsy: #{foo}") rather than (assert
# foo, "foo is falsy: #{foo}") if you want to avoid formatting the
# error message if the assertion is true.
T.assert = (c, stringlike) ->
  if not c
    throw new Error(T.str(stringlike))
# @private
assert = T.assert

# @private
#
# Ways to call this:
# T.check(c, m, x)
# T.check(c, m, x, what)
# T.check(c, m, x, what, wanted, top)
T.check = (c, m, x, what, wanted, top) ->
  if c
    return x
  T.fail(m, x, what, wanted, top)
  throw new Error("unreachable")

# @private
T.safe_to_string = (x) ->
  # 1. Try .toString
  try
    repr = x.toString()
    return repr if repr isnt "[object Object]"
  catch e
    # nothing

  # 2. Try JSON.stringify
  try
    return JSON.stringify(x)
  catch e
    # nothing

  # TODO: try manually detecting cycles or just looping
  # over the top level keys

  # 3. Just type name
  try
    repr = x.constructor.name
    return repr if repr?.match /^[A-Za-z0-9_]+$/
  catch e
    # nothing

  # 4. failed.
  return '[T.safe_to_string failed]'

# @private
T.fail = (m, x, what, wanted, top) ->
  if false
    console.log "m=#{m} #{JSON.stringify(m)}"
    console.log "x=#{x}"
    console.log "x=#{JSON.stringify(x)}"
    console.log "what=#{what} #{JSON.stringify(what)}"
    console.log "wanted=#{wanted} #{JSON.stringify(wanted)}"
    console.log "top=#{top} #{JSON.stringify(top)}"
  if what?
    if wanted?
      if top?
        msg = "Wanted #{T.str(wanted)}, but #{T.str(what)} in #{T.str(top)} #{T.str(m)}"
      else
        msg = "Wanted #{T.str(wanted)}, but #{T.str(what)} #{T.str(m)}"
    else
      msg = "#{T.str(what)} #{T.str(m)}"
  else
    if wanted?
      if top?
        msg = "Wanted #{T.str(wanted)}, but in #{T.str(top)} #{T.str(m)}"
      else
        msg = "Wanted #{T.str(wanted)}, but #{T.str(m)}"
    else
      msg = "#{T.str(m)}"

  e = new Error("#{msg}: #{T.safe_to_string(x)}")
  console.error e
  if false
    window.alert e.stack
  throw e

# @private
T.any = (x, what, wanted, top) ->
  # Anything including undefined.
  return x

# @private
T.defined = (x, what, wanted, top) ->
  if not wanted?
    wanted = "defined"
  T.check(typeof x != "undefined", "is undefined", x, what, wanted, top)
  return x

# @private
T.nonnull = (x, what, wanted, top) ->
  if not wanted?
    wanted = "nonnull"
  T.defined(x, what, wanted, top)
  T.check(x?, "is null", x, what, wanted, top)
  return x

# @private
T.member = (arr) ->
  w = "value in #{JSON.stringify arr}"
  msg = "not in #{JSON.stringify arr}"
  (x, what, wanted, top) ->
    wanted ?= w
    T.check (x in arr), msg, x, what, wanted, top

# @private
T.object = (x, what, wanted, top) ->
  if not wanted?
    wanted = "object"
  T.nonnull(x, what, wanted, top)
  T.check(typeof x == "object", "not an object", x, what, wanted, top)
  return x

# @private
T.bool = (x, what, wanted, top) ->
  if not wanted?
    wanted = "bool"
  T.nonnull(x, what, wanted, top)
  T.check(x == true or x == false, "is not bool", x, what, wanted, top)
  return x

# @private
T.string = (x, what, wanted, top) ->
  if not wanted?
    wanted = "string"
  T.nonnull(x, what, wanted, top)
  T.check(T.is_string(x), "is not a string", x, what, wanted, top)
  return x

# @private
T.num = (x, what, wanted, top) ->
  if not wanted?
    wanted = "num"
  T.nonnull(x, what, wanted, top)
  T.check(typeof x == "number", "is not numeric", x, what, wanted, top)
  return x

# @private
T.int = (x, what, wanted, top) ->
  if not wanted?
    wanted = "int"
  T.num(x, what, wanted, top)
  T.check(x % 1 == 0, "is not an integer", x, what, wanted, top)
  return x

# @private
T.uint = (x, what, wanted, top) ->
  if not wanted?
    wanted = "uint"
  T.int(x, what, wanted, top)
  T.check(x >= 0, "is negative", x, what, wanted, top)
  return x

# @private
T.nullable = (base) ->
  w = "nullable(#{base})"
  fn = (x, what, wanted, top) ->
    if not wanted?
      wanted = () -> w
    T.defined(x, what, wanted, top)
    if x?
      T.get_T_fn(base)(x, what, wanted, top)
    return x
  fn.toString = () -> w
  fn.coerce = (x) -> if x? then T.get_coerce_fn(base)(x) else null
  fn.fromJSON = (x) ->
    if x?
      if base.fromJSON? then (base.fromJSON x) else fn.coerce x
    else
      null
  return fn

# @private
T.array = (x, what, wanted, top) ->
  if not wanted?
    wanted = "array"
  T.nonnull(x, what, wanted, top)
  T.check(T.is_array(x), "is not an array", x, what, wanted, top)
  return x

# @private
T.arrayOf = (elementT) ->
  w = "arrayOf(#{elementT})"
  arrayType = (x, what, wanted, top) ->
    if not wanted?
      wanted = w
    T.array(x, what, wanted, top)
    for elt, i in x
      eltDescription =
        () -> if what? then "element #{i} of #{T.str(what)}" else "element #{i}"
      T.get_T_fn(elementT)(elt, eltDescription, wanted, top)
    return x
  arrayType.toString = () -> w
  arrayType.coerce = (x) ->
    T.array x, null, w
    (T.get_coerce_fn(elementT)(e) for e in x)
  arrayType.fromJSON = (x) ->
    T.array x, 'fromJSON input', w
    if elementT.fromJSON? then ((elementT.fromJSON y) for y in x) else arrayType.coerce x
  return arrayType

# @private
T.instance = (x, type, what, wanted, top) ->
  if not (type instanceof Function)
    throw new Error('Invalid type given: ' + type)
  if not (x instanceof type)
    if not wanted?
      wanted = type.name
    T.check(false, "got instance of " + x?.constructor?.name, x, what, wanted, top)
  return x

# @private
T.unimplemented = (msg) -> -> throw new Error('unimplemented ' + msg)

# @private
T.startsWith = (s, prefix) ->
  return s.lastIndexOf(prefix, 0) == 0

# @private
T.string_matching = (regex_pattern_string) ->
  # We take a string because Regex objects are stateful, and so that
  # we can print it easily.
  T.string regex_pattern_string
  T.check (/^[^].*[$]$/.test regex_pattern_string), "does not start with ^ and end with $",
    regex_pattern_string
  msg = "does not match regex #{regex_pattern_string}"
  return (x, what, wanted, top) ->
    T.string x, what, wanted, top
    T.check ((new RegExp regex_pattern_string).test x), msg, x, what, wanted, top
    return x

# @private
T.is_defined = (x) ->
  return typeof x != "undefined"

# @private
T.is_bool = (x) ->
  return x == true or x == false or (x and (typeof x == "object") and x.constructor == Boolean)

# @private
# note: is_number(NaN) == true
T.is_number = (x) ->
  return (typeof x == "number") or (x and (typeof x == "object") and x.constructor == Number)

# @private
T.is_json_number = (x) ->
  return (T.is_number x) and !(isNaN x) and isFinite x

# @private
T.is_string = (x) ->
  return typeof x == "string" or (x and (typeof x == "object") and x.constructor == String)

# @private
T.is_function = (x) ->
  return typeof x == 'function'

# @private
T.is_object = (x) ->
  return x? and typeof x == 'object'

# @private
T.is_array = (x) ->
  # From http://stackoverflow.com/questions/4775722/javascript-check-if-object-is-array
  return Object.prototype.toString.call(x) == '[object Array]'

# @private
T.is_empty = (x) ->
  return Object.keys(x).length == 0

# @private
T.is_date = (x) ->
  # Similar to is_array.  See also
  # http://stackoverflow.com/questions/1353684/detecting-an-invalid-date-date-instance-in-javascript
  return Object.prototype.toString.call(x) == '[object Date]'

# @private
T.isUint8Array = (x) ->
  return Object.prototype.toString.call(x) == '[object Uint8Array]'

# @private
T.is_simple_map = (x) ->
  if not x? or not (typeof x == "object")
    return false
  for key, value of x
    if not (Object.prototype.hasOwnProperty.call x, key)
      return false
  return true

# @private
# object where all values are own properties
T.simple_map = (x, what, wanted, top) ->
  wanted ?= "simple map"
  T.object x, what, wanted, top
  for key, value of x
    T.check (Object.prototype.hasOwnProperty.call x, key),
      (-> "property #{key} is inherited"), x, what, wanted, x
  return x

# @private
# simple map with specific key and value types.
T.simple_typed_map = (type_name, key_type, value_type) ->
  coerce_key = T.get_coerce_fn key_type
  coerce_value = T.get_coerce_fn value_type
  type = (x, what, wanted, top) ->
    wanted ?= type_name
    T.simple_map x, what, wanted, top
    for key, value of x
      (T.get_T_fn key_type) key, "property", null, x
      (T.get_T_fn value_type) value, (-> "value of property #{key}"), null, x
    return x
  type.coerce = (x) ->
    T.simple_map x, null, type_name
    out = {}
    for k, v of x
      out[coerce_key k] = coerce_value v
    return out
  type.fromJSON = (x) ->
    T.simple_map x, null, type_name
    out = {}
    for k, v of x
      out[coerce_key k] =
        if value_type.fromJSON?
          value_type.fromJSON v
        else
          v
    return out
  return type

# @private
T.DS_ID_REGEX = "^[-_a-z0-9]([-_a-z0-9.]{0,62}[-_a-z0-9])?$|^[.][-_a-zA-Z0-9]{1,63}$"

# @private
# structured sync datastore id
T.dsid = (x, what, wanted, top) ->
  wanted ?= "dsid"
  (T.string_matching T.DS_ID_REGEX) x, what, wanted, top
  return x

# @private
T.SS_ID_REGEX = "^[-._+/=a-zA-Z0-9]{1,64}$|^:[-._+/=a-zA-Z0-9]{1,63}$"

# @private
# structured sync table id
T.tid = (x, what, wanted, top) ->
  wanted ?= "tid"
  (T.string_matching T.SS_ID_REGEX) x, what, wanted, top
  return x

# @private
# structured sync row id
T.rowid = (x, what, wanted, top) ->
  wanted ?= "rowid"
  (T.string_matching T.SS_ID_REGEX) x, what, wanted, top
  return x

# @private
# structured sync field name
T.field_name = (x, what, wanted, top) ->
  wanted ?= "field name"
  (T.string_matching T.SS_ID_REGEX) x, what, wanted, top
  return x


# To make T.nullable(T.int)(...) print better error messages
do ->
  for k, v of T
    if T.hasOwnProperty(k)
      do (k) ->
        v.toString = () -> "T.#{k}"
