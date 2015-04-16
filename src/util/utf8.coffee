Dropbox.Util.countUtf8Bytes = (s) ->
  bytes = 0
  for i in [0...s.length]
    c = s.charCodeAt i
    # see http://en.wikipedia.org/wiki/Comparison_of_Unicode_encodings#Eight-bit_environments
    if c <= 0x7f
      bytes += 1
    else if c <= 0x7FF
      bytes += 2
    else if 0xD800 <= c <= 0xDFFF
      # A surrogate -- half a surrogate pair.  A surrogate pair
      # encodes a code point above 0x10000, which takes 4 UTF-8
      # bytes to encode.  So we count each surrogate as 2 bytes.
      bytes += 2
    else if c <= 0xFFFF
      bytes += 3
    else
      assert false
  return bytes
