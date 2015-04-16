describe 'Dropbox.Util.countUtf8Bytes', ->
  it 'counts ASCII chars', ->
    expect(Dropbox.Util.countUtf8Bytes('')).to.equal 0
    expect(Dropbox.Util.countUtf8Bytes('a')).to.equal 1
    expect(Dropbox.Util.countUtf8Bytes('ab')).to.equal 2
    expect(Dropbox.Util.countUtf8Bytes('abc')).to.equal 3

  it 'counts latin-1 chars', ->
    expect(Dropbox.Util.countUtf8Bytes('ß')).to.equal 2
    expect(Dropbox.Util.countUtf8Bytes('aß')).to.equal 3
    expect(Dropbox.Util.countUtf8Bytes('ßø')).to.equal 4
    expect(Dropbox.Util.countUtf8Bytes('ßøa')).to.equal 5

  it 'counts BMP chars', ->
    expect(Dropbox.Util.countUtf8Bytes('⓶')).to.equal 3

  it 'counts astral plane chars', ->
    expect(Dropbox.Util.countUtf8Bytes('👾')).to.equal 4

  it 'counts a mix of all types', ->
    expect(Dropbox.Util.countUtf8Bytes('aß⓶👾⓶ßa')).to.equal 16
