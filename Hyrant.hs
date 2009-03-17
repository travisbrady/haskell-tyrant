import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString
import qualified Data.ByteString.Lazy.Char8 as LS
import qualified Data.ByteString.Char8 as S
import Data.Binary
import qualified Data.Binary.Get as BG
import Data.Int
import Data.Binary.Put (runPut, putLazyByteString)
import Data.Word (Word8, Word16, Word32)
import qualified Constants as C
import Data.Bits ((.|.))

errorCode 0 = "success"
errorCode 1 = "invalid operation"
errorCode 2 = "host not found"
errorCode 3 = "connection refused"
errorCode 4 = "send error"
errorCode 5 = "recv error"
errorCode 6 = "existing record"
errorCode 7 = "no record found"
errorCode 9999 = "miscellaneous error"

toStrict = S.concat . LS.toChunks
toLazy s = LS.fromChunks [s]

length32 :: LS.ByteString -> Int32
length32 s = fromIntegral $ LS.length s

len32 :: [a] -> Int32
len32 lst = fromIntegral $ length lst

runPS = toStrict . runPut

oneValPut key = do
    put C.magic >> put C.vsiz
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

getDouble sock key = do 
    adddouble sock key 0.0

getInt sock key = do
    addInt sock key 0

putKeep sock key value = do
    let msg = runPS $ makePutKeep key value
    res <- send sock msg
    sockSuccess sock

putCat sock key value = do
    let msg = runPS $ makePutCat key value
    sent <- send sock msg
    sockSuccess sock

sockSuccess sock = do
    rawRetCode <- recv sock 1
    let code = parseRetCode rawRetCode
    return $ errorCode code

out sock key = do
    let msg = runPS $ makeOut key
    sent <- send sock msg
    sockSuccess sock

vsiz sock key = do
    let msg = runPS $ oneValPut key
    res <- send sock msg
    rc <- recv sock 1
    let code = parseRetCode rc
    case code of
        0 -> do
            fetch <- recv sock 4
            return $ Right $ parseLen fetch
        x -> return $ Left $ errorCode x

mget sock keys = do
    let msg = toStrict . runPut $ makeMGet keys
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

makeMGet keys = do
    put C.magic >> put C.mget
    put nkeys
    let z = [(length32 x, x) | x <- keys]
    mapM_ (\(x, y) -> put x >> putLazyByteString y) z
    where nkeys = len32 keys

getManyMGet _ 0 acc = return acc
getManyMGet sock rnum acc = do
    hdr <- recv sock 8
    let (ksize, vsize) = BG.runGet getMGetHeader $ toLazy hdr
    body <- recv sock $ ksize + vsize
    let el = BG.runGet (getOneMGet ksize vsize) $ toLazy body
    getManyMGet sock (rnum-1) (el:acc)

getMGetHeader = do
    rawksize <- BG.getWord32be
    let ksize = (fromEnum rawksize)::Int
    rawvsize <- BG.getWord32be
    let vsize = (fromEnum rawvsize)::Int
    return (ksize, vsize)
    
getOneMGet ksize vsize = do
    k <- BG.getLazyByteString $ toEnum ksize
    v <- BG.getLazyByteString $ toEnum vsize
    return (k, v)

getGet = do
    rawcode <- BG.getWord8
    let code = (fromEnum rawcode)::Int
    --ln is "on success: A 32-bit integer standing for the length of the value"
    ln <- BG.getWord32be
    let len = (fromEnum ln)::Int
    return (code, len)

vanish sock  = justCode sock C.vanish
sync sock = justCode sock C.sync

justCode sock code = do
    let msg = runPS $ (put C.magic >> put code)
    sent <- send sock msg
    sockSuccess sock

copy sock path = do
    let msg = runPS $ oneValPut path
    sent <- send sock msg
    sockSuccess sock

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

parseSize = do
    rawcode <- BG.getWord8
    let code = (fromEnum rawcode)::Int
    rawSize <- BG.getWord64be
    let size = (fromEnum rawSize)::Int
    return (code, size)

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

size sock = sizeOrRNum sock C.size
rnum sock = sizeOrRNum sock C.rnum

stat sock = do
    sent <- send sock $ toStrict . runPut $ (put C.magic >> put C.stat)
    resHeader <- recv sock 5
    let (code, ssiz) = BG.runGet getGet $ toLazy resHeader
    case code of
        0 -> do
            rawStat <- recv sock ssiz
            let statPairs = map (S.split '\t') $ S.lines rawStat
            return $ Just statPairs
        _ -> return Nothing

restore sock path ts = do
    let pl = length32 path
    let ts64 = (fromIntegral ts)::Int64
    let restorePut = (put C.magic >> put C.restore >> put pl >> put ts64 >> putLazyByteString path)
    sent <- send sock $ runPS restorePut
    sockSuccess sock

setmstPut host port = do
    put C.magic >> put C.setmst 
    put hl >> put port32
    putLazyByteString host
    where hl = length32 host
          port32 = (fromIntegral port)::Int32

setmst sock host port = do
    sent <- send sock $ runPS $ setmstPut host port
    sockSuccess sock

integFract :: (RealFrac b) => b -> (Int64, Int64)
integFract num = (integ, fract)
    where integ = (truncate num)
          fract = truncate . (* 1e12) . snd $ properFraction num

parseAddDoubleReponse = do
    integ <- get :: Get Int64
    fract <- get :: Get Int64
    return (integ, fract)

doublePut key integ fract = do
    put C.magic >> put C.adddouble
    put klen >> put integ >> put fract
    putLazyByteString key
    where klen = length32 key

adddouble sock key num = do
    let (integ, fract) = integFract num
    let msg = runPS $ doublePut key integ fract
    sent <- send sock msg
    rawCode <- recv sock 1
    let code = BG.runGet getRetCode $ toLazy rawCode
    case code of
        0 -> do
            fetch <- recv sock 16
            return $ Just $ BG.runGet parseAddDoubleReponse $ toLazy fetch
        _ -> return Nothing

putshlPut key value width = do
    put C.magic >> put C.putshl
    put klen >> put vlen >> put w32
    putLazyByteString key >> putLazyByteString value
    where klen = length32 key
          vlen = length32 value
          w32 = (fromIntegral width)::Int32

putshl sock key value width = do
    let msg = runPS $ putshlPut key value width
    sent <- send sock msg
    rawCode <- recv sock 1
    let code = BG.runGet getRetCode $ toLazy rawCode
    case code of
        0 -> return $ Right True
        x -> return $ Left $ errorCode x

putnrPut = makePuts C.putnr
putnr sock key value = do
    let msg = runPS $ putnrPut key value
    sent <- send sock msg
    return ()

successOrError sock = do
    rawCode <- recv sock 1
    let code = BG.runGet getRetCode $ toLazy rawCode
    case code of
        0 -> return $ Right True
        x -> return $ Left $ errorCode x

iterinit sock = do
    let msg = runPS $ (put C.magic >> put C.iterinit)
    sent <- send sock msg
    successOrError sock

parseLenGet = do
    b <- get :: Get Int32
    let c = (fromIntegral b)::Int
    return c
parseLen s = BG.runGet parseLenGet $ toLazy s

iternext sock = do  
    let msg = runPS $ (put C.magic >> put C.iternext)
    sent <- send sock msg
    rawCode <- recv sock 1
    case (parseRetCode rawCode) of
        0 -> do
            ksizRaw <- recv sock 4
            --let ksiz = BG.runGet parseLen $ toLazy ksizRaw
            let ksiz = parseLen ksizRaw
            kbuf <- recv sock ksiz 
            let klen = (fromIntegral ksiz)::Int64
            let key = BG.runGet (BG.getLazyByteString klen) $ toLazy kbuf
            return $ Right key
        x -> return $ Left $ errorCode x

fwmkeysPut prefix maxKeys = do
    put C.magic >> put C.fwmkeys
    put preflen >> put mx32
    putLazyByteString prefix
    where preflen = length32 prefix
          mx32 = (fromIntegral maxKeys)::Int32

fwmkeys sock prefix maxKeys = do
    let msg = runPS $ fwmkeysPut prefix maxKeys
    sent <- send sock msg
    rawCode <- recv sock 1
    case (parseRetCode rawCode) of
        0 -> do
            knumRaw <- recv sock 4
            --let knum = BG.runGet parseLen $ toLazy knumRaw
            let knum = parseLen knumRaw
            theKeys <- getManyElements sock knum []
            return $ Right theKeys
        x -> return $ Left $ errorCode x

getManyElements _ 0 acc = return acc
getManyElements sock knum acc = do
    klenRaw <- recv sock 4
    let klen = parseLen klenRaw
    keyRaw <- recv sock klen
    let key = BG.runGet (BG.getLazyByteString $ toEnum klen) $ toLazy keyRaw
    getManyElements sock (knum-1) (key:acc)
    
extPut funcname key value opts = do
    put C.magic >> put C.ext
    put nlen >> put opts >> put klen >> put vlen
    putLazyByteString funcname
    putLazyByteString key
    putLazyByteString value
    where nlen = length32 funcname
          klen = length32 key
          vlen = length32 value

optOr :: [Int32] -> Int32
optOr [] = 0
optOr opts = foldl1 (.|.) opts

readLazy nb s = BG.runGet (BG.getLazyByteString $ toEnum nb) $ toLazy s
parseCode s = BG.runGet getRetCode $ toLazy s
ext sock funcname key value opts = do
    let msg = runPS $ extPut funcname key value $ optOr opts
    sent <- send sock msg
    rc <- recv sock 1
    let rcp = parseCode rc
    case rcp of
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

main = do
    let k = LS.pack "hab"
    let v = LS.pack "blab"
    s <- openConnect "localhost" "1978"
    b <- putValue s k v
    bump <- getValue s k
    print bump
    let k2 = LS.pack "test"
    let v2 = LS.pack "gabi"
    pkd <- putKeep s k v2
    redid <- getValue s k
    --Now slap another "blab" on the end
    catd <- putCat s k v
    --show
    hey <- getValue s k
    --add another pair
    yodo <- putValue s k2 v2
    print yodo
    zoop <- getValue s k2
    print zoop
    --use out to kill the (k2, v2) pair
    killed <- out s k2
    print killed    -- should be True
    -- Try to fetch value which we've just killed, should be Nothing
    zoop2 <- getValue s k2
    print zoop2
    valSize <- vsiz s k
    print valSize
    let keys = [LS.pack "yex", LS.pack "one", k]
    pairs <- mget s keys
    let outPath = LS.pack "/home/travis/hogo.tch"
    copyConfirm <- copy s outPath
    print copyConfirm
    --areTheyGone <- vanish s
    let k3 = LS.pack "dude"
    let v3 = runPut $ put (0::Int32)
    pd <- putValue s k3 v3
    zap <- getValue s k3
    jungle <- addInt s k3 (20::Int)
    sz <- size s
    numrecs <- rnum s
    stats <- stat s
    let k5 = LS.pack "dubval"
    let v5 = LS.pack "500"
    randy <- putValue s k5 v5
    --let v5 = 5.5
    dubx <- adddouble s k5 5.5
    let k6 = LS.pack "blah"
    dud <- adddouble s k6 10.005
    sally <- getValue s k6
    let k7 = LS.pack "k7"
    swoop <- addInt s k7 1
    hump <- getInt s k7
    let k8 = LS.pack "argo"
    jump <- getDouble s k8
    let k9 = LS.pack "blogo"
    i0 <- getInt s k9
    inext <- addInt s k9 1
    let k10 = LS.pack "sub"
    let v10 = LS.pack "v10"
    blub <- putshl s k10 v10 10
    let k11 = LS.pack "k11"
    let v11 = LS.pack "v11"
    pnrd <- putnr s k11 v11
    gog <- iterinit s
    ham <- iternext s
    sam <- iternext s
    let k12 = LS.pack "k12"
    let v12 = LS.pack "v11"
    blump <- putValue s k12 v12
    fmks <- fwmkeys s (LS.pack "k") 10
    let fname = LS.pack "fibonacci"
    ext0 <- ext s fname k12 v12 []
    misky <- misc s (LS.pack "getlist") [LS.pack "k12"] []
    let keyList = map LS.pack ["k14", "v14", "k15", "v15"]
    masky <- misc s (LS.pack "putlist") keyList []
    sClose s
    return masky
