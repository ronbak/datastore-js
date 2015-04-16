describe 'Dropbox.Datastore', ->
  int64 = Dropbox.Datastore.int64

  test_cases = [
        [ true , true , true],
        [ true , false , false],
        [ false , true , false],
        [ false , false , true],
        [ true , new Boolean(true) , true],
        [ true , new Boolean(false) , false],
        [ false , new Boolean(true) , false],
        [ false , new Boolean(false) , true],
        [ new Boolean(true) , true , true],
        [ new Boolean(false) , true , false],
        [ new Boolean(true) , false , false],
        [ new Boolean(false) , false , true],
        [ new Boolean(true) , new Boolean(true) , true],
        [ new Boolean(false) , new Boolean(true) , false],
        [ new Boolean(true) , new Boolean(false) , false],
        [ new Boolean(false) , new Boolean(false) , true],

        [ "a" , "a" , true],
        [ "a" , "b" , false],
        [ "a" , new String("a") , true],
        [ "a" , new String("a hate boxed values") , false],
        [ new String("a") , "a" , true],
        [ new String("a") , "b" , false],
        [ new String("a") , new String("a") , true],
        [ new String("a") , new String("b") , false],
        
        [ 1 , 1 , true ],
        [ 1 , 0 , false ],
        [ new Number(1) , 1 , true ],
        [ new Number(1) , 0 , false ],
        [ new Number(1) , new Number(1) , true ],
        [ new Number(1) , new Number(0) , false ],
        
        [ (int64 "1") , (int64 "1") , true ],
        [ (int64 "1") , (int64 "0") , false ],
        [ (int64 (new String "1")) , (int64 "1") , true ],
        [ (int64 (new String "1")) , (int64 "0") , false ],
        [ (int64 "9223372036854775807") ,
          (int64 "9223372036854775807") , true ],
        [ (int64 "9223372036854775807") ,
          (int64 "0") , false ],
        [ (int64 "9223372036854775001") ,
          (int64 "9223372036854775000") , false ],

        [ 1 , (int64 "1") , true ],
        [ 1 , (int64 "0") , false ],
        [ new Number(1) , (int64 "1") , true ],
        [ new Number(1) , (int64 "0") , false ],
        [ 1.5 , (int64 "1") , false ],
        [ 1.5 , (int64 "0") , false ],
        [ 9223372036854775001 , (int64 "9223372036854775000") , true ],
        [ 9223372036854775000 , (int64 "9223372036854775001") , true ], # not bug
        [ (int64 "9223372036854775001"), 9223372036854775000  , true ], # not bug
        [ (int64 "9223372036854775000"), 9223372036854775000  , true ], # not bug

        [ 0/0 , 0/0 , true ],
        [ 0/0 , 1 , false ],
        [ 0/0 , 1/0 , false ],
        [ 0/0 , new Number(0/0) , true],
        [ 0/0 , new Number(1) , false],
        [ 0/0 , new Number(1/0) , false],
        [ 0/0 , {N: 'nan'} , true],

        [ new Number(0/0) , 0/0 , true ],
        [ new Number(0/0) , 1 , false ],
        [ new Number(0/0) , 1/0 , false ],
        [ new Number(0/0) , new Number(0/0) , true],
        [ new Number(0/0) , new Number(1) , false],
        [ new Number(0/0) , new Number(1/0) , false],
        [ new Number(0/0) , {N: 'nan'} , true],

        [ 1/0 , 0/0 , false ],
        [ 1/0 , 1 , false ],
        [ 1/0 , 1/0 , true ],
        [ 1/0 , (-1)/0 , false ],
        [ 1/0 , new Number(0/0) , false],
        [ 1/0 , new Number(1) , false],
        [ 1/0 , new Number(1/0) , true],
        [ 1/0 , {N: '+inf'} , true ],
        [ -1/0 , {N: '-inf'} , true ],

        [ 1/0 , 0/0 , false ],
        [ 1/0 , 1 , false ],
        [ 1/0 , 1/0 , true ],
        [ 1/0 , (-1)/0 , false ],
        [ 1/0 , new Number(0/0) , false],
        [ 1/0 , new Number(1) , false],
        [ 1/0 , new Number(1/0) , true],
        [ 1/0 , {N: '+inf'} , true ],
        [ -1/0 , {N: '-inf'} , true ],

        [ new Number(1/0) , 0/0 , false ],
        [ new Number(1/0) , 1 , false ],
        [ new Number(1/0) , 1/0 , true ],
        [ new Number(1/0) , (-1)/0 , false ],
        [ new Number(1/0) , new Number(0/0) , false],
        [ new Number(1/0) , new Number(1) , false],
        [ new Number(1/0) , new Number(1/0) , true],
        [ new Number(1/0) , {N: '+inf'} , true ],
        [ new Number(-1/0) , {N: '-inf'} , true ],

        [ new Date(1373165865935) , {T: "1373165865935"} , true ]
        [ new Date(1373165865935) , {T: "1373165865934"} , false ]

        # TODO: Not exhaustive
        [ [ 1, 2, 3 ] , [ 1, 2, 3 ] , true],
        [ [ 1, 2, 0/0 ] , [ 1, 2, {N: 'nan'} ] , true]
        [ [ 1, 2, 3 ] , [ 1, 2, "3" ] , false ]
        
        ]

  it 'matching for queries works', (done) ->
    match = Dropbox.Datastore.impl.matchDsValues

    for [pattern, value, result] in test_cases
      output = match {field: pattern}, {field: value}
      expect(output).to.equal result

    done()
