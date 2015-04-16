# Wraps datastore client RPC methods with retry logic.
#
# TODO: give this a better name, or, ideally, write a new "connection
# manager" that has a more holistic perspective of whether we're
# connected, what RPCs we want to send, what RPCs recently failed,
# etc., rather than treating each RPC independently as we do here.

# @private
class Backoff
  constructor: () ->
    @min_delay_millis = 500
    @max_delay_millis = 90000
    @base = 1.5
    @_failures = 0
    @log = false

  set_log: (@log) ->

  set_max_delay_millis: (@max_delay_millis) ->

  get_backoff_millis: ->
    @_failures += 1
    target_delay_millis = Math.min(@max_delay_millis, @min_delay_millis * Math.pow(@base, @_failures - 1))
    delay_millis = (.5 + Math.random()) * target_delay_millis
    if @log
      console.log "get_backoff_millis: failures=#{@_failures}, target_delay_millis=#{target_delay_millis}, delay_millis=#{delay_millis}"
    return delay_millis

  reset: ->
    @_failures = 0


# @private
# TODO(dropbox): add an interface for retrying immediately
class RetryWithBackoff
  DEFAULT_GIVEUP_AFTER_MS = 60 * 1000

  COUNTER = 0

  constructor: ->
    @backoff = new Backoff

  # simulates calling to_run with an argument of cb, except that for
  # certain errors the call will be retried.
  #
  # options.do_retry takes the form (err) -> boolean, where the
  # boolean indicates whether to retry for that kind of error; by
  # default it always returns true
  #
  # options.giveup_after_ms specifies a duration in milliseconds after
  # which no further attempts will be made, and cb is called with the
  # last error encountered
  #
  # returns a function which, if called, cancels the run
  run: (to_run, options, cb) ->
    do_retry = options.do_retry ? (-> true)
    giveup_after_ms = options.giveup_after_ms ? DEFAULT_GIVEUP_AFTER_MS
    giveup_threshold = Date.now() + giveup_after_ms

    cancelled = false
    attempt = =>
      return if cancelled
      to_run (err, more_args...) =>
        return if cancelled
        if err and (do_retry err)
          if Date.now() > giveup_threshold
            console.error "Giving up due to error", err
            return cb err
          to_wait = @backoff.get_backoff_millis()
          console.warn "Retrying in #{to_wait} ms due to error", err
          setTimeout attempt, to_wait
        else
          return cb err, more_args...
    attempt()
    return (-> cancelled = true)

# @private
class FlobClient
  # @private
  ONLINE_OP_MAX_RETRY_SECS = 10
  # @private
  OFFLINE_OP_MAX_RETRY_SECS = 60 * 60 * 24 * 7 * 4

  # @private
  constructor: (@client) ->
    @_retry = new RetryWithBackoff

  # body must take one arg, a callback, which it will call with a
  # first argument of err, possibly followed by other args.  Those
  # args (together with err, if every attempt was unsuccessful) will
  # be passed on to cb.
  _run_with_retries: (give_up_after_seconds, cb, body) ->
    options =
      giveup_after_ms: (1000 * give_up_after_seconds)
      do_retry: (err) ->
        return err.status == 0 or (500 <= err.status < 600)
    @_retry.run body, options, cb

  # cb params: err
  delete_db: (handle, cb) ->
    # Handle uniquely identifies a particular datastore instance, so
    # this would be safe to retry for a longer time.  Still, we only
    # retry for a bit, to be conservative, and since we don't have a
    # sound strategy for offline operation.
    @_run_with_retries ONLINE_OP_MAX_RETRY_SECS, cb, (cb) =>
      @client._deleteDatastore handle, (err, resp) =>
        return cb err if err?
        return cb null, resp

  # cb params: err, ListDatastoresResponse
  list_dbs: (cb) ->
    # TODO: figure out if this should be online or offline
    @_run_with_retries OFFLINE_OP_MAX_RETRY_SECS, cb, (cb) =>
      @client._listDatastores (err, resp) ->
        return cb err if err?
        cb null, resp

  # cb params: err, CreateDatastoreResponse
  get_or_create_db: (dsid, cb) ->
    @_run_with_retries ONLINE_OP_MAX_RETRY_SECS, cb, (cb) =>
      @client._getOrCreateDatastore dsid, (err, resp) ->
        return cb err if err?
        return cb null, resp

  # cb params: err, CreateDatastoreResponse
  create_db: (dsid, key, cb) ->
    @_run_with_retries ONLINE_OP_MAX_RETRY_SECS, cb, (cb) =>
      @client._createDatastore dsid, key, (err, resp) ->
        return cb err if err?
        return cb null, resp

  # cb params: err, GetDatastoreResponse
  get_db: (dsid, cb) ->
    @_run_with_retries ONLINE_OP_MAX_RETRY_SECS, cb, (cb) =>
      @client._getDatastore dsid, (err, resp) ->
        return cb err if err?
        return cb null, resp

  # Calls cb err, AwaitResponse
  await: (handle_version_map, db_list_token, cb) ->
    cancel_fn = @_run_with_retries OFFLINE_OP_MAX_RETRY_SECS, cb, (cb) =>
      @client._datastoreAwait handle_version_map, db_list_token, (err, resp) =>
        return cb err if err?
        cb null, resp
    return cancel_fn

  # cb params: err
  put_delta: (handle, flob_delta, cb) ->
    @_run_with_retries OFFLINE_OP_MAX_RETRY_SECS, cb, (cb) =>
      @client._putDelta handle, flob_delta, (err, resp) =>
        return cb err if err?
        return cb null, resp

  # Calls cb err, resp
  get_snapshot: (handle, cb) ->
    @_run_with_retries ONLINE_OP_MAX_RETRY_SECS, cb, (cb) =>
      @client._getSnapshot handle, (err, resp) =>
        return cb err if err?
        return cb null, resp
