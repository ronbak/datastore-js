impl = Dropbox.Datastore.impl

describe 'fromDsValue', ->
  it 'works for empty blob', ->
    x = Dropbox.Datastore.impl.fromDsValue null, null, null, { B: '' }
    expect(x.length).to.equal 0

describe 'clone', ->
  it 'works for null', ->
    expect(impl.clone null).to.equal null

describe 'dbase64FromBase64', ->
  it 'works for simple cases', ->
    expect(impl.dbase64FromBase64 '+/asd==').to.equal '-_asd'

  it 'works for inputs with multiple + signs', ->
    expect(impl.dbase64FromBase64 'asd++zxc').to.equal 'asd--zxc'

describe 'uint8ArrayFromBase64String', ->
  it 'works for input with multiple - and _ characters', ->
    expect(impl.uint8ArrayFromBase64String('--__')).to.deep.equal new Uint8Array [251, 239, 255]
    expect(impl.base64StringFromUint8Array([251, 239, 255])).to.equal '--__'
