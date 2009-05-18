-- |
-- Module       : Database.TokyoTyrant
-- Copyright    : (c) Travis Brady 2009
--
-- License      : BSD-style
-- Maintainer   : travis.brady@gmail.com
-- Stability    : Experimental
--  
-- A pure Haskell interface to the Tokyo Tyrant database server
--
module Database.TokyoTyrant 
    (TyrantOption(RecordLocking, GlobalLocking, NoUpdateLog)
    ,TokyoTyrantHandle
    ,openConnection
    ,closeConnection
    ,putValue
    ,getValue
    ,getDouble
    ,putDouble
    ,getInt
    ,putInt
    ,putKeep
    ,putCat
    ,out
    ,vsiz
    ,mget
    ,vanish
    ,sync
    ,copy
    ,addInt
    ,size
    ,rnum
    ,stat
    ,restore
    ,setmst
    ,addDouble
    ,putshl
    ,putnr
    ,iterinit
    ,iternext
    ,fwmkeys
    ,ext
    ,misc) where

import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString.Lazy
import qualified Data.ByteString.Lazy.Char8 as LS
import Data.Binary
import qualified Data.Binary.Get as BG
import Data.Binary.Put (runPut, putLazyByteString, PutM)
import Data.Int
import Data.Word (Word8, Word16, Word32)
import Data.Bits ((.|.))

import qualified Database.TokyoTyrant.Constants as C

data TyrantOption = RecordLocking   -- `RDBXOLCKREC' for record locking
                    | GlobalLocking -- `RDBXOLCKGLB' for global locking
                    | NoUpdateLog   -- `RDBMONOULOG' for omission of the update log
    deriving (Eq, Show)

newtype TokyoTyrantHandle = TokyoTyrantHandle Socket

ttsend :: TokyoTyrantHandle -> LS.ByteString -> IO Int64
ttsend (TokyoTyrantHandle sock) str = send sock str

ttrecv :: TokyoTyrantHandle -> Int64 -> IO LS.ByteString
ttrecv (TokyoTyrantHandle sock) len = recv sock len

-- | Convert TyrantOption type to Int32
optToInt32 :: TyrantOption -> Int32
optToInt32 RecordLocking = C.rDBXOLCKREC
optToInt32 GlobalLocking = C.rDBXOLCKGLB 
optToInt32 NoUpdateLog   = C.rDBMONOULOG 

-- | Convert Tokyo Tyrant error codes to string representation
errorCode :: Int -> String
errorCode 0 = "success"
errorCode 1 = "invalid operation"
errorCode 2 = "host not found"
errorCode 3 = "connection refused"
errorCode 4 = "send error"
errorCode 5 = "recv error"
errorCode 6 = "existing record"
errorCode 7 = "no record found"
errorCode 9999 = "miscellaneous error"

length32 :: LS.ByteString -> Int32
length32 s = fromIntegral $ LS.length s

len32 :: [a] -> Int32
len32 lst = fromIntegral $ length lst

-- | Create a Put with two params
oneValPut :: (Binary t) => t -> LS.ByteString -> PutM ()
oneValPut code key = do
    put C.magic >> put code
    put klen >> putLazyByteString key
    where klen = length32 key

makePuts :: Word8 -> LS.ByteString -> LS.ByteString -> Put
makePuts code key value = do
    put C.magic >> put code
    put klen >> put vlen
    putLazyByteString key >> putLazyByteString value
    where klen = length32 key
          vlen = length32 value

makePut :: LS.ByteString -> LS.ByteString -> Put
makePut key value = makePuts C.put key value

makePutKeep :: LS.ByteString -> LS.ByteString -> Put
makePutKeep key value = makePuts C.putkeep key value

makePutCat :: LS.ByteString -> LS.ByteString -> Put
makePutCat key value = makePuts C.putcat key value

makeGet :: LS.ByteString -> Put
makeGet key = do
    put C.magic >> put C.get
    put (length32 key) >> putLazyByteString key

getRetCode :: Get Int
getRetCode = do
    rawCode <- BG.getWord8
    let ret = (fromEnum rawCode)::Int
    return ret

tcpHints = defaultHints {addrFamily = AF_INET, addrProtocol = 6}

-- | Connect to Tokyo Tyrant
openConnection :: HostName -> ServiceName -> IO TokyoTyrantHandle
openConnection hostname port = do
    addrinfos <- getAddrInfo (Just tcpHints) (Just hostname) (Just port)
    let addr = head addrinfos
    sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    setSocketOption sock NoDelay 1
    connect sock (addrAddress addr)
    return $ TokyoTyrantHandle sock

-- | Close connection to ttserver
closeConnection :: TokyoTyrantHandle -> IO ()
closeConnection (TokyoTyrantHandle sock) = sClose sock

parseRetCode :: LS.ByteString -> Int
parseRetCode = BG.runGet getRetCode

simpleSuccess (TokyoTyrantHandle sock) = do
    rc <- recv sock 1
    case (parseRetCode rc) of
        0 -> return . Right $ errorCode 0
        x -> return . Left $ errorCode x

-- | Store a record
putValue :: TokyoTyrantHandle -> LS.ByteString -> LS.ByteString -> IO (Either String String)
putValue handle key value = do
    let msg = runPut $ makePut key value
    res <- ttsend handle msg
    simpleSuccess handle

-- | Retrieve a record
getValue :: TokyoTyrantHandle -> LS.ByteString -> IO (Either String LS.ByteString)
getValue handle key = do
    let msg = runPut $ makeGet key
    res <- ttsend handle msg
    rc <- ttrecv handle 1
    let code = parseRetCode rc
    case code of
        0 -> do
            vl <- ttrecv handle 4
            let valLen = (fromIntegral $ parseLen vl)::Int64
            rawValue <- ttrecv handle valLen
            return $ Right rawValue
        x -> return $ Left $ errorCode x

-- | Store a record where the value is a double
putDouble :: TokyoTyrantHandle -> LS.ByteString -> Double -> IO (Either [Char] Double)
putDouble handle key value = do
    out handle key
    addDouble handle key value

getDouble :: TokyoTyrantHandle -> LS.ByteString -> IO (Either [Char] Double)
getDouble handle key = do 
    addDouble handle key 0.0

-- | Store a record with an Int value
putInt :: TokyoTyrantHandle -> LS.ByteString -> Int -> IO (Either [Char] Int)
putInt handle key value = do
    out handle key
    addInt handle key value

-- | Retrieve a record with an Int value
getInt :: TokyoTyrantHandle -> LS.ByteString -> IO (Either [Char] Int)
getInt handle key = do
    addInt handle key 0

-- | Store a new record
--   If key already exists nothing is done
putKeep :: TokyoTyrantHandle
           -> LS.ByteString
           -> LS.ByteString
           -> IO (Either String String)
putKeep handle key value = do
    let msg = runPut $ makePutKeep key value
    res <- ttsend handle msg
    simpleSuccess handle

-- | Concatenate a value at the end of the existing record
putCat :: TokyoTyrantHandle
          -> LS.ByteString
          -> LS.ByteString
          -> IO (Either String String)
putCat handle key value = do
    let msg = runPut $ makePutCat key value
    sent <- ttsend handle msg
    simpleSuccess handle

-- | Remove a record
out :: TokyoTyrantHandle -> LS.ByteString -> IO (Either String String)
out handle key = do
    let msg = runPut $ oneValPut C.out key
    sent <- ttsend handle msg
    simpleSuccess handle

-- | Get the size of the value of a record
vsiz :: TokyoTyrantHandle -> LS.ByteString -> IO (Either [Char] Int)
vsiz handle key = do
    let msg = runPut $ oneValPut C.vsiz key
    res <- ttsend handle msg
    rc <- ttrecv handle 1
    let code = parseRetCode rc
    case code of
        0 -> do
            fetch <- ttrecv handle 4
            return $ Right $ parseLen fetch
        x -> return $ Left $ errorCode x

-- | Fetch keys and values for multiple records
mget :: TokyoTyrantHandle
        -> [LS.ByteString]
        -> IO (Either [Char] [(LS.ByteString, LS.ByteString)])
mget handle keys = do
    let msg = runPut $ mgetPut keys
    res <- ttsend handle msg
    rc <- ttrecv handle 1
    let code = parseRetCode rc
    case code of
        0 -> do
            rnumRaw <- ttrecv handle 4
            let rnum = parseLen rnumRaw
            pairs <- getManyMGet handle rnum []
            return $ Right pairs
        x -> return $ Left $ errorCode x

mgetPut :: [LS.ByteString] -> PutM ()
mgetPut keys = do
    put C.magic >> put C.mget
    put nkeys
    let z = [(length32 x, x) | x <- keys]
    mapM_ (\(x, y) -> put x >> putLazyByteString y) z
    where nkeys = len32 keys

getManyMGet :: (Num t) =>
               TokyoTyrantHandle
               -> t
               -> [(LS.ByteString, LS.ByteString)]
               -> IO [(LS.ByteString, LS.ByteString)]
getManyMGet _ 0 acc = return acc
getManyMGet handle rnum acc = do
    hdr <- ttrecv handle 8
    let (ksize, vsize) = BG.runGet getMGetHeader hdr
    body <- ttrecv handle $ ksize + vsize
    let el = BG.runGet (getOneMGet ksize vsize) body
    getManyMGet handle (rnum-1) (el:acc)

getMGetHeader :: Get (Int64, Int64)
getMGetHeader = do
    rawksize <- BG.getWord32be
    let ksize = (fromIntegral rawksize)::Int64
    rawvsize <- BG.getWord32be
    let vsize = (fromIntegral rawvsize)::Int64
    return (ksize, vsize)
    
getOneMGet :: Int64 -> Int64 -> Get (LS.ByteString, LS.ByteString)
getOneMGet ksize vsize = do
    k <- BG.getLazyByteString ksize
    v <- BG.getLazyByteString vsize
    return (k, v)

-- | Remove all records
vanish :: TokyoTyrantHandle -> IO (Either String String)
vanish handle = justCode handle C.vanish

-- | Synchronize updated contents with the database file
sync :: TokyoTyrantHandle -> IO (Either String String)
sync handle = justCode handle C.sync

justCode :: (Binary t) => TokyoTyrantHandle -> t -> IO (Either String String)
justCode handle code = do
    let msg = runPut $ (put C.magic >> put code)
    sent <- ttsend handle msg
    simpleSuccess handle

-- | Copy the database file to the specified path
copy :: TokyoTyrantHandle -> LS.ByteString -> IO (Either String String)
copy handle path = do
    let msg = runPut $ oneValPut C.copy path
    sent <- ttsend handle msg
    simpleSuccess handle 

-- | Add an integer to a record
addInt :: (Integral a) =>
          TokyoTyrantHandle -> LS.ByteString -> a -> IO (Either [Char] Int)
addInt handle key x = do
    let wx = (fromIntegral x)::Int32
    let klen = length32 key
    let msg = runPut (put C.magic >> put C.addint >> put klen >> put wx >> putLazyByteString key)
    sent <- ttsend handle msg
    rc <- ttrecv handle 1
    let code = parseRetCode rc
    case code of
        0 -> do
            sumraw <- ttrecv handle 4
            let thesum = parseLen sumraw
            return $ Right thesum
        x -> return $ Left $ errorCode x

parseSize :: Get Int
parseSize = do
    rawSize <- BG.getWord64be
    let size = (fromEnum rawSize)::Int
    return size

sizeOrRNum :: (Binary t) => TokyoTyrantHandle -> t -> IO (Either [Char] Int)
sizeOrRNum handle cmdId = do
    let msg = runPut (put C.magic >> put cmdId)
    sent <- ttsend handle msg
    rc <- ttrecv handle 1
    let code = parseRetCode rc
    case code of
        0 -> do
            sizeraw <- ttrecv handle 8
            let size = BG.runGet parseSize sizeraw
            return $ Right size
        x -> return $ Left $ errorCode x

-- | Get the size of the database
size :: TokyoTyrantHandle -> IO (Either [Char] Int)
size handle = sizeOrRNum handle C.size

-- | Get the number of records
rnum :: TokyoTyrantHandle -> IO (Either [Char] Int)
rnum handle = sizeOrRNum handle C.rnum

-- | Get the stats string
stat :: TokyoTyrantHandle -> IO (Either [Char] [[LS.ByteString]])
stat handle = do
    sent <- ttsend handle $ runPut (put C.magic >> put C.stat)
    rc <- ttrecv handle 1
    let code = parseRetCode rc
    case code of
        0 -> do
            ssizRaw <- ttrecv handle 4
            let ssiz = (fromIntegral $ parseLen ssizRaw)::Int64
            statRaw <- ttrecv handle ssiz
            let statPairs = map (LS.split '\t') $ LS.lines statRaw
            return $ Right statPairs
        x -> return $ Left $ errorCode x

-- | Restore the database with update log
restore :: (Integral a) =>
           TokyoTyrantHandle -> LS.ByteString -> a -> IO (Either String String)
restore handle path ts = do
    let pl = length32 path
    let ts64 = (fromIntegral ts)::Int64
    let restorePut = (put C.magic >> put C.restore >> put pl >> put ts64 >> putLazyByteString path)
    sent <- ttsend handle $ runPut restorePut
    simpleSuccess handle 

setmstPut :: (Integral a) => LS.ByteString -> a -> PutM ()
setmstPut host port = do
    put C.magic >> put C.setmst 
    put hl >> put port32
    putLazyByteString host
    where hl = length32 host
          port32 = (fromIntegral port)::Int32

-- | Set the replication master
setmst handle host port = do
    sent <- ttsend handle $ runPut $ setmstPut host port
    simpleSuccess handle 

integFract :: (RealFrac b) => b -> (Int64, Int64)
integFract num = (integ, fract)
    where integ = (truncate num)
          fract = truncate . (* 1e12) . snd $ properFraction num

pairToDouble :: (Int64, Int64) -> Double
pairToDouble (integ, fract) = integDouble + (fractDouble*1e-12)
    where integDouble = fromIntegral integ
          fractDouble = fromIntegral fract

parseAddDoubleReponse :: Get (Int64, Int64)
parseAddDoubleReponse = do
    integ <- get :: Get Int64
    fract <- get :: Get Int64
    return (integ, fract)

doublePut :: (Binary t, Binary t1) => LS.ByteString -> t -> t1 -> PutM ()
doublePut key integ fract = do
    put C.magic >> put C.adddouble
    put klen >> put integ >> put fract
    putLazyByteString key
    where klen = length32 key

-- | Add a real number to a record
addDouble handle key num = do
    let (integ, fract) = integFract num
    let msg = runPut $ doublePut key integ fract
    sent <- ttsend handle msg
    rc <- ttrecv handle 1
    let code = parseRetCode rc
    case code of
        0 -> do
            fetch <- ttrecv handle 16
            let pair = BG.runGet parseAddDoubleReponse fetch
            return . Right $ pairToDouble pair
        x -> return . Left $ errorCode x

putshlPut :: (Integral a) =>
             LS.ByteString -> LS.ByteString -> a -> PutM ()
putshlPut key value width = do
    put C.magic >> put C.putshl
    put klen >> put vlen >> put w32
    putLazyByteString key >> putLazyByteString value
    where klen = length32 key
          vlen = length32 value
          w32 = (fromIntegral width)::Int32

-- | concatenate a value at the end of the existing record and shift it to the lef
putshl :: (Integral a) =>
          TokyoTyrantHandle            -- ^ Connection
          -> LS.ByteString  -- ^ key
          -> LS.ByteString  -- ^ value
          -> a              -- ^ width
          -> IO (Either String String)
putshl handle key value width = do
    let msg = runPut $ putshlPut key value width
    sent <- ttsend handle msg
    simpleSuccess handle

putnrPut :: LS.ByteString -> LS.ByteString -> Put
putnrPut = makePuts C.putnr

-- | store a record into a remote database object without response from the server
putnr :: TokyoTyrantHandle -> LS.ByteString -> LS.ByteString -> IO ()
putnr handle key value = do
    let msg = runPut $ putnrPut key value
    sent <- ttsend handle msg
    return ()

-- | initialize the iterator of a remote database object
iterinit :: TokyoTyrantHandle -> IO (Either String String)
iterinit handle = do
    let msg = runPut $ (put C.magic >> put C.iterinit)
    sent <- ttsend handle msg
    simpleSuccess handle

parseLenGet :: Get Int
parseLenGet = do
    b <- get :: Get Int32
    let c = (fromIntegral b)::Int
    return c

parseLen :: LS.ByteString -> Int
parseLen s = BG.runGet parseLenGet s

-- | get the next key of the iterator of a remote database object
iternext :: TokyoTyrantHandle -> IO (Either [Char] LS.ByteString)
iternext handle = do
    let msg = runPut $ (put C.magic >> put C.iternext)
    sent <- ttsend handle msg
    rawCode <- ttrecv handle 1
    case (parseRetCode rawCode) of
        0 -> do
            ksizRaw <- ttrecv handle 4
            let ksiz = (fromIntegral $ parseLen ksizRaw)::Int64
            kbuf <- ttrecv handle ksiz
            let klen = (fromIntegral ksiz)::Int64
            let key = BG.runGet (BG.getLazyByteString klen) $ kbuf
            return $ Right key
        x -> return $ Left $ errorCode x

fwmkeysPut :: (Integral a) => LS.ByteString -> a -> PutM ()
fwmkeysPut prefix maxKeys = do
    put C.magic >> put C.fwmkeys
    put preflen >> put mx32
    putLazyByteString prefix
    where preflen = length32 prefix
          mx32 = (fromIntegral maxKeys)::Int32

-- | get forward matching keys in a remote database object
fwmkeys :: (Integral a) =>
           TokyoTyrantHandle -> LS.ByteString -> a -> IO (Either [Char] [LS.ByteString])
fwmkeys handle prefix maxKeys = do
    let msg = runPut $ fwmkeysPut prefix maxKeys
    sent <- ttsend handle msg
    rawCode <- ttrecv handle 1
    case (parseRetCode rawCode) of
        0 -> do
            knumRaw <- ttrecv handle 4
            let knum = parseLen knumRaw
            theKeys <- getManyElements handle knum []
            return $ Right theKeys
        x -> return $ Left $ errorCode x

getManyElements :: (Num t) => TokyoTyrantHandle -> t -> [LS.ByteString] -> IO [LS.ByteString]
getManyElements _ 0 acc = return acc
getManyElements handle knum acc = do
    klenRaw <- ttrecv handle 4
    let klen = (fromIntegral $ parseLen klenRaw)::Int64
    keyRaw <- ttrecv handle klen
    let key = BG.runGet (BG.getLazyByteString klen) keyRaw
    getManyElements handle (knum-1) (key:acc)
    
extPut :: LS.ByteString
          -> LS.ByteString
          -> LS.ByteString
          -> Int32
          -> PutM ()
extPut funcname key value opts = do
    put C.magic >> put C.ext
    put nlen >> put opts >> put klen >> put vlen
    putLazyByteString funcname
    putLazyByteString key
    putLazyByteString value
    where nlen = length32 funcname
          klen = length32 key
          vlen = length32 value

optOr :: [TyrantOption] -> Int32
optOr [] = 0
optOr opts = foldl1 (.|.) $ map optToInt32 opts

readLazy :: Int64 -> LS.ByteString -> LS.ByteString
readLazy nb s = BG.runGet (BG.getLazyByteString nb) s

parseCode :: LS.ByteString -> Int
parseCode s = BG.runGet getRetCode s

-- | Call a function of the script language extension
ext :: TokyoTyrantHandle               -- ^ Connection to Tokyo Tyrant
       -> LS.ByteString     -- ^ the lua function to be called
       -> LS.ByteString     -- ^ specifies the key
       -> LS.ByteString     -- ^ specified the value
       -> [TyrantOption]    -- ^ locking and update log options
       -> IO (Either [Char] LS.ByteString)
ext handle funcname key value opts = do
    let msg = runPut $ extPut funcname key value $ optOr opts
    sent <- ttsend handle msg
    rc <- ttrecv handle 1
    case (parseCode rc) of
        0 -> do
            rsizRaw <- ttrecv handle 4
            let rsiz = (fromIntegral $ parseLen rsizRaw)::Int64
            rbuf <- ttrecv handle rsiz
            let result = readLazy rsiz rbuf
            return $ Right result
        x -> return $ Left $ errorCode x

miscPut funcname args opts = do
    put C.magic >> put C.misc
    put nlen >> put opts >> put rnum
    putLazyByteString funcname
    mapM_ (\arg -> put (length32 arg) >> putLazyByteString arg) args
    where nlen = length32 funcname
          rnum = len32 args

-- | Call a versatile function for miscellaneous operations
-- funcname can be "getlist", "putlist" and "outlist"
-- getlist takes a list of keys and returns a list of values
-- putlist takes a list of keys and values one after the other and returns []
-- outlist takes a list of keys and removes those records
misc :: TokyoTyrantHandle              -- ^ Connection to Tokyo Tyrant
        -> LS.ByteString    -- ^ funcname
        -> [LS.ByteString]  -- ^ args
        -> [TyrantOption]   -- ^ options
        -> IO (Either [Char] [LS.ByteString])
misc handle funcname args opts = do
    let msg = runPut $ miscPut funcname args $ optOr opts
    sent <- ttsend handle msg
    rc <- ttrecv handle 1
    let rcp = parseCode rc
    case rcp of
        0 -> do
            rnumRaw <- ttrecv handle 4
            let rnum = parseLen rnumRaw
            elements <- getManyElements handle rnum []
            return $ Right elements
        x -> return $ Left $ errorCode x

