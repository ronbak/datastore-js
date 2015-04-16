# @private
impl.clone = (x) ->
  # breaks for NaN and Infinity
  #return JSON.parse JSON.stringify obj
  if x instanceof Array
    ((impl.clone e) for e in x)
  else if x? and typeof x is 'object'
    out = {}
    for k, v of x
      out[k] = impl.clone v
    out
  else
    x

# @private
impl.WEB64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

# @private
impl.randomElement = (s) ->
  s[Math.floor(Math.random() * s.length)]

# @private
impl.randomWeb64String = (len) ->
  (impl.randomElement(impl.WEB64_ALPHABET) for i in [0...len]).join("")

# @private
impl.uint8ArrayFromBase64String = (s) ->
  # the input lacks = padding, but atob seems to be OK with it
  # replace web-friendly - and _ with + and /
  s = s.replace(/-/g, '+').replace(/_/g, '/')
  byteString = Dropbox.Util.atob s
  len = byteString.length
  out = new Uint8Array len
  for i in [0...len]
    out[i] = byteString.charCodeAt i
  return out

impl.dbase64FromBase64 = (s) ->
  # replace + with -, / with _, and strip = padding
  return s.replace(/[+]/g, '-').replace(/[/]/g, '_').replace(/[\=]+$/g, '')

# @private
impl.base64StringFromUint8Array = (bytes) ->
  byteString = ""
  for byte in bytes
    byteString += String.fromCharCode byte
  s = Dropbox.Util.btoa byteString
  return impl.dbase64FromBase64 s

# @private
impl.INT64_TAG = 'dbxInt64'

# @private
impl.isInt64 = (x) ->
  unless x and typeof x == 'object' and x.constructor == Number and isFinite x
    return false
  tag = x[impl.INT64_TAG]
  unless (T.is_string tag) and (tag == '0' or impl.nonzero_int64_approximate_regex.test tag)
    return false
  return true

# @private
impl.validateInt64 = (x) ->
  if not x and (typeof x == 'object' and x.constructor == Number and isFinite x)
    throw new Error "Not a finite boxed number: #{x}"
  tag = x[impl.INT64_TAG]
  if not ((T.is_string tag) and (tag == '0' or impl.nonzero_int64_approximate_regex.test tag))
    throw new Error "Missing or invalid tag in int64: #{tag}"
  parsed = (parseInt tag, 10)
  if parsed != Number x
    throw new Error "Tag in int64 does not match value #{Number x}: #{tag}"
  return x

# @private
impl.toDsValue = (x, allowArray = true) ->
  if (x == null) or (typeof x == 'undefined')
    throw new Error "Bad value: #{x}"
  else if T.is_string x
    return x
  else if T.is_bool x
    return x
  else if T.is_number x
    if x[impl.INT64_TAG]?
      impl.validateInt64 x
      return { I: x[impl.INT64_TAG] }
    else if isFinite x
      return x
    else if isNaN x
      return { N: "nan" }
    else if (Number x) == Infinity
      return { N: "+inf" }
    else if (Number x) == -Infinity
      return { N: "-inf" }
    else
      throw new Error "Unexpected number: #{x}"
  else if T.is_array x
    if not allowArray
      throw new Error "Nested array not allowed: #{JSON.stringify x}"
    else
      return ((impl.toDsValue y, false) for y in x)
  else if T.is_date x
    # Dates are supposed to be integers, but round anyway to make sure
    # we don't generate a bad delta.
    # http://www.ecma-international.org/ecma-262/5.1/#sec-15.9.1.1
    t = Math.round x.getTime()
    return { T: "#{t}" }
  else if T.isUint8Array x
    return { B: impl.base64StringFromUint8Array x }
  # Datastore.List is not allowed, at least for now; seems like it could be
  # confusing.  The developer can be explicit and use Datastore.List.toArray
  # and pass that to set().
  else
    throw new Error "Unexpected value: #{T.safe_to_string x}"

# @private
impl.fromDsValue = (datastore, record, fieldName, x) ->
  # Not much validation in this direction -- we assume no bad values
  # can get in.
  if T.is_string x
    return x
  else if T.is_bool x
    return x
  else if T.is_number x
    return x
  else if T.is_array x
    return new Dropbox.Datastore.List datastore, record, fieldName
  else
    if typeof x != 'object'
      throw new Error "Unexpected value: #{x}"
    if x.I?
      return Dropbox.Datastore.int64 x.I
    else if x.N?
      switch x.N
        when 'nan'
          return NaN
        when '+inf'
          return Infinity
        when '-inf'
          return -Infinity
        else
          throw new Error "Unexpected object: #{JSON.stringify x}"
    else if x.B?
      return impl.uint8ArrayFromBase64String x.B
    else if x.T?
      return new Date (parseInt x.T, 10)
    else
      throw new Error "Unexpected object: #{JSON.stringify x}"

impl.matchDsValues = (pattern, data) ->
  fieldMatch = (field_pattern, field_data) ->
    if not field_pattern?
      throw new Error "Unexpected object: #{field_pattern}"

    if not field_data?
      return false

    return fieldEq field_pattern, field_data

  # Tests whether Javascript value jsval is equal to Datastores value dsval.
  # Treats longs and doubles as interconvertible where possible.
  #
  # There's no guarantee that the value of dsval is reasonable
  fieldEq = (jsval, dsval) ->
    # Check that jsval is sane
    impl.toDsValue jsval

    if (T.is_string jsval) and (T.is_string dsval)
      return (String jsval) == (String dsval)
    else if (T.is_bool jsval) and (T.is_bool dsval)
      jsval = jsval.valueOf() if (typeof jsval) == "object"
      dsval = dsval.valueOf() if (typeof dsval) == "object"
      return (Boolean jsval) == (Boolean dsval)
    else if (T.is_number jsval) and ((T.is_number dsval) or (dsval.N?) or (dsval.I?))
      # a does not need to be normalized because the user won't be giving us
      # un-normalized values (or, shouldn't be)
      dsval = impl.fromDsValue undefined, undefined, undefined, dsval

      # I personally disagree with this decision, but both mixed comparisons
      # coerce to doubles.
      if jsval[impl.INT64_TAG] and dsval[impl.INT64_TAG]
        [jsval, dsval] = [(Dropbox.Datastore.int64 jsval), (Dropbox.Datastore.int64 dsval)]
        return (String jsval[impl.INT64_TAG]) == (String dsval[impl.INT64_TAG])
      else if (isNaN jsval) and (isNaN dsval)
        return true
      else
        return (Number jsval) == (Number dsval)
    else if (T.is_array jsval) and (T.is_array dsval)
      return false if jsval.length != dsval.length
      for i in [0..jsval.length-1]
        return false if not fieldMatch(jsval[i], dsval[i])
      return true
    else if (T.is_date jsval) and ((T.is_date dsval) or dsval.T?)
      if dsval.T?
        dsval = impl.fromDsValue undefined, undefined, undefined, dsval

      return (jsval-0) == (dsval-0)
    else if (T.isUint8Array jsval) and ((T.isUint8Array dsval) or dsval.B?)
      if dsval.B?
        dsval = impl.fromDsValue undefined, undefined, undefined, dsval

      return false if jsval.length != dsval.length
      for i in [0..jsval.length-1]
        return false if jsval[i] != dsval[i]
      return true
    else
      return false

  for k, v of pattern
    test = fieldMatch v, data[k]
    return test if not test
  return true


# @private
class RecordCache
  constructor: (@_datastore) ->
    # Map of tids to map of rids to DbxRecord objects.
    @_cache = {}

  get: (tid, rid) ->
    if not @_cache[tid]?
      return null
    return @_cache[tid][rid]

  getOrCreate: (tid, rid) ->
    if not @_cache[tid]?
      @_cache[tid] = {}
    record = @_cache[tid][rid]
    if not record?
      record = @_cache[tid][rid] = new Dropbox.Datastore.Record @_datastore, tid, rid
    return record

  remove: (tid, rid) ->
    delete @_cache[tid][rid]
    if T.is_empty @_cache[tid]
      delete @_cache[tid]
    undefined


# @private
#
# Keeps track of which listeners are registered with which
# EventSources and makes it easy to remove them.
class EventManager
  constructor: ->
    @_registered_handlers = []

  register: (src, handler) ->
    src.addListener handler
    @_registered_handlers.push [src, handler]
    undefined

  unregister_all: ->
    for [src, handler] in @_registered_handlers
      src.removeListener handler
    undefined
