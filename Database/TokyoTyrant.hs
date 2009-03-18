module Database.TokyoTyrant 
    (TyrantOption(RecordLocking, GlobalLocking, NoUpdateLog)
    ,openConnect
    ,close
    ,putValue
    ,getValue
    ,getDouble
    ,getInt
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
    ,adddouble
    ,putshl
    ,putnr
    ,iterinit
    ,iternext
    ,fwmkeys
    ,ext
    ,misc)
    where

import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import qualified Data.ByteString.Lazy.Char8 as LS
import qualified Data.ByteString.Char8 as S
import Data.Binary
import qualified Data.Binary.Get as BG
import Data.Binary.Put (runPut, putLazyByteString, PutM)
import Data.Int
import Data.Word (Word8, Word16, Word32)
import qualified Database.TokyoTyrant.Constants as C
import Data.Bits ((.|.))

data TyrantOption = RecordLocking   -- `RDBXOLCKREC' for record locking
                    | GlobalLocking -- `RDBXOLCKGLB' for global locking
                    | NoUpdateLog   -- `RDBMONOULOG' for omission of the update log
    deriving (Eq, Show)

optToInt32 :: TyrantOption -> Int32
optToInt32 RecordLocking = C.rDBXOLCKREC
optToInt32 GlobalLocking = C.rDBXOLCKGLB 
optToInt32 NoUpdateLog   = C.rDBMONOULOG 

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

toStrict :: LS.ByteString -> S.ByteString
toStrict = S.concat . LS.toChunks

toLazy :: S.ByteString -> LS.ByteString
toLazy s = LS.fromChunks [s]

runPS :: Put -> S.ByteString
runPS = toStrict . runPut

length32 :: LS.ByteString -> Int32
length32 s = fromIntegral $ LS.length s

len32 :: [a] -> Int32
len32 lst = fromIntegral $ length lst

oneValPut :: (Binary t) => t -> LS.ByteString -> PutM ()
oneValPut code key = do
    put C.magic >> put code
    put klen >> putLazyByteString key
    where klen = length32 key

makeOut :: LS.ByteString -> Put
makeOut key = do
    put C.magic >> put C.out
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

openConnect :: HostName -> ServiceName -> IO Socket
openConnect hostname port = do
    addrinfos <- getAddrInfo Nothing (Just hostname) (Just port)
    let addr = head addrinfos
    sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    setSocketOption sock NoDelay 1
    connect sock (addrAddress addr)
    return sock

close :: Socket -> IO ()
close sock = sClose sock

parseRetCode :: S.ByteString -> Int
parseRetCode = BG.runGet getRetCode . toLazy

putValue :: Socket
            -> LS.ByteString
            -> LS.ByteString
            -> IO (Either [Char] Bool)
putValue sock key value = do
    let msg = runPS $ makePut key value
    res <- send sock msg
    rc <- recv sock 1
    let retCode = parseRetCode rc
    case retCode of
        0 -> return $ Right True
        x -> return $ Left $ errorCode x

getValue :: Socket -> LS.ByteString -> IO (Either String LS.ByteString)
getValue sock key = do
    let msg = runPS $ makeGet key
    res <- send sock msg
    rc <- recv sock 1
    let code = parseRetCode rc
    case code of
        0 -> do
            vl <- recv sock 4
            let valLen = parseLen vl
            rawValue <- recv sock valLen
            return $ Right $ toLazy rawValue
        x -> return $ Left $ errorCode x

getDouble :: Socket -> LS.ByteString -> IO (Either [Char] Double)
getDouble sock key = do 
    adddouble sock key 0.0

getInt :: Socket -> LS.ByteString -> IO (Either [Char] Int)
getInt sock key = do
    addInt sock key 0

putKeep :: Socket -> LS.ByteString -> LS.ByteString -> IO [Char]
putKeep sock key value = do
    let msg = runPS $ makePutKeep key value
    res <- send sock msg
    sockSuccess sock

putCat :: Socket -> LS.ByteString -> LS.ByteString -> IO [Char]
putCat sock key value = do
    let msg = runPS $ makePutCat key value
    sent <- send sock msg
    sockSuccess sock

sockSuccess :: Socket -> IO [Char]
sockSuccess sock = do
    rawRetCode <- recv sock 1
    let code = parseRetCode rawRetCode
    return $ errorCode code

out :: Socket -> LS.ByteString -> IO [Char]
out sock key = do
    let msg = runPS $ makeOut key
    sent <- send sock msg
    sockSuccess sock

vsiz :: Socket -> LS.ByteString -> IO (Either [Char] Int)
vsiz sock key = do
    let msg = runPS $ oneValPut C.vsiz key
    res <- send sock msg
    rc <- recv sock 1
    let code = parseRetCode rc
    case code of
        0 -> do
            fetch <- recv sock 4
            return $ Right $ parseLen fetch
        x -> return $ Left $ errorCode x

mget :: Socket
        -> [LS.ByteString]
        -> IO (Either [Char] [(LS.ByteString, LS.ByteString)])
mget sock keys = do
    let msg = toStrict . runPut $ mgetPut keys
    res <- send sock msg
    rc <- recv sock 1
    let code = parseRetCode rc
    case code of
        0 -> do
            rnumRaw <- recv sock 4
            let rnum = parseLen rnumRaw
            pairs <- getManyMGet sock rnum []
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
               Socket
               -> t
               -> [(LS.ByteString, LS.ByteString)]
               -> IO [(LS.ByteString, LS.ByteString)]
getManyMGet _ 0 acc = return acc
getManyMGet sock rnum acc = do
    hdr <- recv sock 8
    let (ksize, vsize) = BG.runGet getMGetHeader $ toLazy hdr
    body <- recv sock $ ksize + vsize
    let el = BG.runGet (getOneMGet ksize vsize) $ toLazy body
    getManyMGet sock (rnum-1) (el:acc)

getMGetHeader :: Get (Int, Int)
getMGetHeader = do
    rawksize <- BG.getWord32be
    let ksize = (fromEnum rawksize)::Int
    rawvsize <- BG.getWord32be
    let vsize = (fromEnum rawvsize)::Int
    return (ksize, vsize)
    
getOneMGet :: Int -> Int -> Get (LS.ByteString, LS.ByteString)
getOneMGet ksize vsize = do
    k <- BG.getLazyByteString $ toEnum ksize
    v <- BG.getLazyByteString $ toEnum vsize
    return (k, v)

getGet :: Get (Int, Int)
getGet = do
    rawcode <- BG.getWord8
    let code = (fromEnum rawcode)::Int
    --ln is "on success: A 32-bit integer standing for the length of the value"
    ln <- BG.getWord32be
    let len = (fromEnum ln)::Int
    return (code, len)

vanish :: Socket -> IO [Char]
vanish sock  = justCode sock C.vanish

sync :: Socket -> IO [Char]
sync sock = justCode sock C.sync

justCode :: (Binary t) => Socket -> t -> IO [Char]
justCode sock code = do
    let msg = runPS $ (put C.magic >> put code)
    sent <- send sock msg
    sockSuccess sock

copy :: Socket -> LS.ByteString -> IO [Char]
copy sock path = do
    let msg = runPS $ oneValPut C.copy path
    sent <- send sock msg
    sockSuccess sock

addInt :: (Integral a) =>
          Socket -> LS.ByteString -> a -> IO (Either [Char] Int)
addInt sock key x = do
    let wx = (fromIntegral x)::Int32
    let klen = length32 key
    let msg = runPS $ (put C.magic >> put C.addint >> put klen >> put wx >> putLazyByteString key)
    sent <- send sock msg
    rc <- recv sock 1
    let code = parseRetCode rc
    case code of
        0 -> do
            sumraw <- recv sock 4
            let thesum = parseLen sumraw
            return $ Right thesum
        x -> return $ Left $ errorCode x

parseSize :: Get Int
parseSize = do
    rawSize <- BG.getWord64be
    let size = (fromEnum rawSize)::Int
    return size

sizeOrRNum :: (Binary t) => Socket -> t -> IO (Either [Char] Int)
sizeOrRNum sock cmdId = do
    let msg = runPS $ (put C.magic >> put cmdId)
    sent <- send sock msg
    rc <- recv sock 1
    let code = parseRetCode rc
    case code of
        0 -> do
            sizeraw <- recv sock 8
            let size = BG.runGet parseSize $ toLazy sizeraw
            return $ Right size
        x -> return $ Left $ errorCode x

size :: Socket -> IO (Either [Char] Int)
size sock = sizeOrRNum sock C.size

rnum :: Socket -> IO (Either [Char] Int)
rnum sock = sizeOrRNum sock C.rnum

stat :: Socket -> IO (Either [Char] [[S.ByteString]])
stat sock = do
    sent <- send sock $ toStrict . runPut $ (put C.magic >> put C.stat)
    rc <- recv sock 1
    let code = parseRetCode rc
    case code of
        0 -> do
            ssizRaw <- recv sock 4
            let ssiz = parseLen ssizRaw
            statRaw <- recv sock ssiz
            let statPairs = map (S.split '\t') $ S.lines statRaw
            return $ Right statPairs
        x -> return $ Left $ errorCode x

restore :: (Integral a) =>
           Socket -> LS.ByteString -> a -> IO [Char]
restore sock path ts = do
    let pl = length32 path
    let ts64 = (fromIntegral ts)::Int64
    let restorePut = (put C.magic >> put C.restore >> put pl >> put ts64 >> putLazyByteString path)
    sent <- send sock $ runPS restorePut
    sockSuccess sock

setmstPut :: (Integral a) => LS.ByteString -> a -> PutM ()
setmstPut host port = do
    put C.magic >> put C.setmst 
    put hl >> put port32
    putLazyByteString host
    where hl = length32 host
          port32 = (fromIntegral port)::Int32

setmst :: (Integral a) => Socket -> LS.ByteString -> a -> IO [Char]
setmst sock host port = do
    sent <- send sock $ runPS $ setmstPut host port
    sockSuccess sock

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

doublePut :: (Binary t, Binary t1) =>
             LS.ByteString -> t -> t1 -> PutM ()
doublePut key integ fract = do
    put C.magic >> put C.adddouble
    put klen >> put integ >> put fract
    putLazyByteString key
    where klen = length32 key

adddouble sock key num = do
    let (integ, fract) = integFract num
    let msg = runPS $ doublePut key integ fract
    sent <- send sock msg
    rc <- recv sock 1
    let code = parseRetCode rc
    case code of
        0 -> do
            fetch <- recv sock 16
            let pair = BG.runGet parseAddDoubleReponse $ toLazy fetch
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

putshl :: (Integral a) =>
          Socket
          -> LS.ByteString
          -> LS.ByteString
          -> a
          -> IO (Either [Char] Bool)
putshl sock key value width = do
    let msg = runPS $ putshlPut key value width
    sent <- send sock msg
    rawCode <- recv sock 1
    let code = BG.runGet getRetCode $ toLazy rawCode
    case code of
        0 -> return $ Right True
        x -> return $ Left $ errorCode x

putnrPut :: LS.ByteString -> LS.ByteString -> Put
putnrPut = makePuts C.putnr

putnr :: Socket -> LS.ByteString -> LS.ByteString -> IO ()
putnr sock key value = do
    let msg = runPS $ putnrPut key value
    sent <- send sock msg
    return ()

iterinit :: Socket -> IO [Char]
iterinit sock = do
    let msg = runPS $ (put C.magic >> put C.iterinit)
    sent <- send sock msg
    sockSuccess sock

parseLenGet :: Get Int
parseLenGet = do
    b <- get :: Get Int32
    let c = (fromIntegral b)::Int
    return c

parseLen :: S.ByteString -> Int
parseLen s = BG.runGet parseLenGet $ toLazy s

iternext :: Socket -> IO (Either [Char] LS.ByteString)
iternext sock = do  
    let msg = runPS $ (put C.magic >> put C.iternext)
    sent <- send sock msg
    rawCode <- recv sock 1
    case (parseRetCode rawCode) of
        0 -> do
            ksizRaw <- recv sock 4
            let ksiz = parseLen ksizRaw
            kbuf <- recv sock ksiz 
            let klen = (fromIntegral ksiz)::Int64
            let key = BG.runGet (BG.getLazyByteString klen) $ toLazy kbuf
            return $ Right key
        x -> return $ Left $ errorCode x

fwmkeysPut :: (Integral a) => LS.ByteString -> a -> PutM ()
fwmkeysPut prefix maxKeys = do
    put C.magic >> put C.fwmkeys
    put preflen >> put mx32
    putLazyByteString prefix
    where preflen = length32 prefix
          mx32 = (fromIntegral maxKeys)::Int32

fwmkeys :: (Integral a) =>
           Socket -> LS.ByteString -> a -> IO (Either [Char] [LS.ByteString])
fwmkeys sock prefix maxKeys = do
    let msg = runPS $ fwmkeysPut prefix maxKeys
    sent <- send sock msg
    rawCode <- recv sock 1
    case (parseRetCode rawCode) of
        0 -> do
            knumRaw <- recv sock 4
            let knum = parseLen knumRaw
            theKeys <- getManyElements sock knum []
            return $ Right theKeys
        x -> return $ Left $ errorCode x

getManyElements :: (Num t) => Socket -> t -> [LS.ByteString] -> IO [LS.ByteString]
getManyElements _ 0 acc = return acc
getManyElements sock knum acc = do
    klenRaw <- recv sock 4
    let klen = parseLen klenRaw
    keyRaw <- recv sock klen
    let key = BG.runGet (BG.getLazyByteString $ toEnum klen) $ toLazy keyRaw
    getManyElements sock (knum-1) (key:acc)
    
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

readLazy :: Int -> S.ByteString -> LS.ByteString
readLazy nb s = BG.runGet (BG.getLazyByteString $ toEnum nb) $ toLazy s

parseCode :: S.ByteString -> Int
parseCode s = BG.runGet getRetCode $ toLazy s

ext :: Socket
       -> LS.ByteString
       -> LS.ByteString
       -> LS.ByteString
       -> [TyrantOption]
       -> IO (Either [Char] LS.ByteString)
ext sock funcname key value opts = do
    let msg = runPS $ extPut funcname key value $ optOr opts
    sent <- send sock msg
    rc <- recv sock 1
    case (parseCode rc) of
        0 -> do
            rsizRaw <- recv sock 4
            let rsiz = parseLen rsizRaw
            rbuf <- recv sock rsiz
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

misc :: Socket
        -> LS.ByteString
        -> [LS.ByteString]
        -> [TyrantOption]
        -> IO (Either [Char] [LS.ByteString])
misc sock funcname args opts = do
    let msg = runPS $ miscPut funcname args $ optOr opts
    sent <- send sock msg
    rc <- recv sock 1
    let rcp = parseCode rc
    case rcp of
        0 -> do
            rnumRaw <- recv sock 4
            let rnum = parseLen rnumRaw
            elements <- getManyElements sock rnum []
            return $ Right elements
        x -> return $ Left $ errorCode x

