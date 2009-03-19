Pure Haskell interface to Tokyo Tyrant
======================================

haskell-tyrant lets you connect to ttserver

Example
-------

A simple example assuming you've got ttserver running at the default location
    
    module Main where

    import Database.TokyoTyrant
    import Data.ByteString.Lazy.Char8 (pack)

    defaultHost = "localhost"
    defaultPort = "1978"

    main = do
        let k = pack "mykey"
        let v = pack "myval"
        conn <- openConnection defaultHost defaultPort
        result <- putValue conn k v
        --should be "success"
        print result
        g <- getValue conn k
        print g
        -- remove the record created above
        out conn k
        -- close connection to server
        closeConnection conn
