# A struct is a simple record type.  It behaves similarly to a
# CoffeeScript class, but properties of instances are strictly
# type-checked, and the set of methods is just a predefined set.
#
# For now, type-checking only happens during construction and during
# serialization with JSON.stringify() (to avoid putting garbage on the
# wire or into storage).  We don't have checked getters/setters yet,
# should perhaps add them.
#
# Struct definitions also serve as a place to put comments on what the
# struct is used for, its invariants, etc.
#
# Struct instances behave almost the same as plain JavaScript objects.
#
# A struct's constructor takes one argument, a Javascript object with
# the same properties as the struct.  The constructor checks that the
# set of fields and their types exactly match the struct.
#
# Other static fields:
#
#   .fromJSON() method that ignores unknown fields
#   for forward/backward compatibility on the wire.
#
#   .Type is a T-style type assertion.
#
# The declaration syntax is
#    user = struct.define('user', [
#      ['uid', T.uid],
#      ['email', T.string],
#    ])
# The type of a property is either a type assertion function from T or
# a struct.
#
# I chose this syntax over something like
#    user = struct.define('user', {
#      uid: T.uid
#      email: T.string
#    })
# since it preserves the order of declaration of fields.

# For now, recursive types are not allowed.

# It might be interesting to have these types be structural rather
# than nominal, and to make struct.Type() a coercion function rather
# than a checking function.

# TODO: avoid O(n^2) (or worse?) type checking for nested types

impl.struct = struct = {}

# @private
struct.define = (stname, fields_arg) ->
  T.string(stname, "struct name")
  T.array(fields_arg, "fields")
  # double underscores to avoid clashes with local variables
  __fields = []
  __field_map = {}
  for f, i in fields_arg
    T.array(f, "field", "field descriptor", fields_arg)
    T.check(2 <= f.length && f.length <= 3, "does not have length 2 or 3",
      f, "field descriptor")
    name = T.string(f[0], "field name", "field descriptor", fields_arg)
    type = T.nonnull(f[1], "field type", "field descriptor", fields_arg)
    options = if f.length <= 2 then {} else
      T.nonnull(f[2], "map of field options", "field descriptor", fields_arg)
    for k of options
      if k not in ['init', 'initFn']
        T.fail("unknown option #{k}", options, "field options", "field descrptor", fields_arg)
    # 'default' is a reserved word, so we use 'init' for now.
    if 'init' in options and 'initFn' in options
      T.fail("both 'init' and 'initFn' specified", options, "field options", "field descriptor", fields_arg)
    initFn = if 'initFn' of options
      options.initFn
    else if 'init' of options
      init = options.init
      do (init) ->
        -> init
    else
      null
    args = {name: name, type: type, initFn: initFn}
    if StructField?
      field = new StructField(args)
    else
      # bootstrap
      field = args
    __fields.push(field)
    __field_map[name] = field
  msg = "initializer for #{stname} (fields #{((f.name for f in __fields)).join(', ')})"

  clazz = (x) ->
    T.defined(x, "x", "initializer")
    for name, value of x
      if x.hasOwnProperty(name)
        T.check(__field_map[name]?, (-> "has an unexpected field #{name}"), x, "initializer")
    for field in __fields
      if x[field.name] and not x.hasOwnProperty(field.name)
        T.fail("Has an indirect property #{field.name}", x, "initializer")
      if not x.hasOwnProperty(field.name)
        if field.initFn?
          @[field.name] = field.initFn()
        else
          T.fail("lacks the field #{field.name}", x, "initializer")
      else
        value = x[field.name]
        @[field.name] = T.get_coerce_fn(field.type)(value)
    # kind of hacky
    clazz.Type(@, "initializer", msg, @)
    return @

  clazz.Type = (x, what, wanted, top) ->
    T.defined(x, what, wanted, top)
    T.check(x instanceof clazz, (-> "is not an instance of #{stname}"), x, what, wanted, top)
    # Verify fields.
    for field in __fields
      T.check(x.hasOwnProperty(field.name), (-> "lacks the field #{field.name}"),
        x, what, wanted, top)
      T.get_T_fn(field.type)(x[field.name], field.name, wanted, top)
    for name, value of x
      if x.hasOwnProperty(name)
        T.check(__field_map[name]?, "has an unexpected field", name, what, wanted, top)
    return x

  clazz.coerce = (x) ->
    if x instanceof clazz
      # Verify fields.
      clazz.Type(x)
      return x
    else
      return new clazz(x)

  clazz.prototype.toString = ->
    obj = this
    data = []
    # Use order of declaration.
    for field in __fields
      v = obj[field.name]
      data.push "#{field.name}: " +
        if (T.is_object v) and (T.is_function v.toString)
          v.toString()
        else
          JSON.stringify v
    return "{#{data.join(', ')}}"

  clazz.prototype.toJSON = ->
    obj = this
    # Avoid stringification through JSON.stringify since that would
    # lead to infinite recursion.
    str = -> "#{obj}"
    for field in __fields
      T.get_T_fn(field.type)(this[field.name], field.name, null, str)
    return this

  clazz.fromJSON = (x) ->
    T.simple_map x, "input"
    fields = {}
    for k, v of x
      field = __field_map[k]
      if field?
        type = field.type
        if type.fromJSON?
          fields[k] = type.fromJSON v
        else
          fields[k] = v
    return new clazz fields

  clazz.toString = -> "struct #{stname}"

  return clazz

StructField = struct.define('StructField', [
  ['name', T.string]
  ['type', T.defined],
  # A zero-arg function.  If an initializer does not provide a default
  # value, and initFn is non-null, initFn will be invoked, and its
  # return value will be used to initialize the field.
  ['initFn', T.defined],
])


# @private
#
# Returns an object with the same properties but not toString etc.
# For testing with jasmine expect().toEqual().
struct.toJSO = (x) ->
  if typeof x != 'object'
    return x
  if T.is_array(x)
    return (struct.toJSO(e) for e in x)
  else
    out = {}
    for k, v of x
      if x.hasOwnProperty(k)
        out[k] = struct.toJSO(v)
    return out



# Basic idea:
#
# Maybe = struct.union_as_list 'Maybe', [
#   ['Some', [['value', T.nonnull]]]
#   ['None', []]
# ]
#
# defines Maybe.Type and Maybe.coerce to make Maybe a usable "type" in
# our little "type system".  It also defines Maybe.Some and Maybe.None
# as "hacked" structs -- structs that are modified to serialize into
# arrays.  Maybe() is not a constructor; the way to get a Maybe
# instance is to call Maybe.from_array() with an array, or to invoke
# the struct constructors Maybe.Some() or Maybe.None().
#
# Hacked structs also have a tag() method that returns the tag.

# @private
struct.union_as_list = (uname, variants_arg) ->
  T.string uname, "union name"
  T.array variants_arg, "variants"

  union = ->
    # Maybe we could just call from_array here, but I'm not confident
    # about returning a different type (that isn't even a subclass).
    # Should we make it a subclass, perhaps?
    throw new Error "Use #{uname}.from_array instead"

  # Map of tags to hacked structs.
  __variants_map = {}
  # array to preserve order
  __tags = []
  for v in variants_arg
    T.array v, "variant", "variant descriptor", variants_arg
    T.check v.length == 2, "does not have length 2", v, "variant descriptor", variants_arg
    __tag = T.string v[0], "tag", "tag", variants_arg
    __fields = T.array v[1], "fields", "variant descriptor", variants_arg
    __struct_fields = __fields.slice 0
    __struct_fields.unshift ['_tag', T.member [__tag]]

    do (__tag, __fields, __struct_fields) ->

      hacked_struct = struct.define "#{uname}.#{__tag}", __struct_fields

      hacked_struct.prototype.tag = ->
        return __tag

      hacked_struct.prototype.toString = ->
        return "#{uname}.#{__tag}(#{JSON.stringify @})"

      hacked_struct.prototype.toJSON = ->
        accu = [__tag]
        for field in __fields
          name = field[0]
          type = field[1]
          (T.get_T_fn type)(@[name], name)
          accu.push @[name]
        return accu

      hacked_struct.from_array = (x) ->
        wanted = "initializer for #{uname}"
        T.array x, "initializer", wanted
        T.check x.length == __fields.length + 1, "does not have length #{__fields.length + 1}",
          x, "initializer", wanted
        T.check x[0] == __tag, "does not have tag #{__tag}", x, "initializer", wanted
        accu = {_tag: __tag}
        for field, i in __fields
          name = field[0]
          accu[name] = x[i + 1]
        return new hacked_struct accu

      hacked_struct.fromJSON = (x) ->
        if x.length > __fields.length + 1
          x = x.slice 0, (__fields.length + 1)
        return hacked_struct.from_array x

      hacked_struct.coerce = (x) ->
        if x instanceof hacked_struct
          # Verify fields.
          hacked_struct.Type x
          return x
        else
          return hacked_struct.from_array x

      __variants_map[__tag] = hacked_struct
      union[__tag] = hacked_struct

    __tags.push __tag

  msg = "initializer for #{uname} (variants #{__tags.join(', ')})"

  union.from_array = (x) ->
    wanted = "initializer for #{uname}"
    T.array x, "initializer", wanted
    T.check x.length >= 1, "lacks a tag", x, "initializer", wanted
    tag = x[0]
    T.string tag, "tag", wanted, x
    (T.member __tags) tag
    return __variants_map[tag].from_array x

  union.fromJSON = (x) ->
    wanted = "initializer for #{uname}"
    T.array x, "initializer", wanted
    T.check x.length >= 1, "lacks a tag", x, "initializer", wanted
    tag = x[0]
    T.string tag, "tag", wanted, x
    (T.member __tags) tag
    return __variants_map[tag].fromJSON x

  union.Type = (x, what, wanted, top) ->
    wanted ?= "#{uname}.Type"
    T.defined x, what, wanted, top
    T.defined x.tag, "tag", wanted, top
    tag = x.tag()
    T.string tag, "tag", "initializer", x
    (T.member __tags) tag
    __variants_map[tag].Type x, null, "object of type #{uname}"
    return x

  union.coerce = (x) ->
    for t, s of __variants_map
      if x instanceof s
        # Verify fields.
        s.Type x
        return x
    return union.from_array x

  union.toString = -> "union #{uname}"

  return union
