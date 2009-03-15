import Control.Monad
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

toStrict = S.concat . LS.toChunks
toLazy s = LS.fromChunks [s]

length32 :: LS.ByteString -> Int32
length32 s = fromIntegral $ LS.length s

len32 :: [a] -> Int32
len32 lst = fromIntegral $ length lst

makeVsiz :: LS.ByteString -> Put
makeVsiz key = do
    put C.magic
    put C.vsiz
    let klen = length32 key
    put klen

makeOut :: LS.ByteString -> Put
makeOut key = do
    put C.magic
    put C.out
    let klen = length32 key
    put klen

makePuts :: Word8 -> LS.ByteString -> LS.ByteString -> Put
makePuts code key value = do
    put C.magic
    put code
    let klen = length32 key
    let vlen = length32 value
    put klen >> put vlen

makePut :: LS.ByteString -> LS.ByteString -> Put
makePut key value = makePuts C.put key value

makePutKeep :: LS.ByteString -> LS.ByteString -> Put
makePutKeep key value = makePuts C.putkeep key value

makePutCat :: LS.ByteString -> LS.ByteString -> Put
makePutCat key value = makePuts C.putcat key value

makeGet :: LS.ByteString -> Put
makeGet key = do
    put C.magic
    put C.get
    put (length32 key)

getRetCode = do
    rawCode <- BG.getWord8
    let ret = (fromEnum rawCode)::Int
    return ret

openConnect :: HostName -> ServiceName -> IO Socket
openConnect hostname port = do
    addrinfos <- getAddrInfo Nothing (Just hostname) (Just port)
    let addr = head addrinfos
    sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    connect sock (addrAddress addr)
    return sock

putValue :: Socket -> LS.ByteString -> LS.ByteString -> IO Bool
putValue sock key value = do
    let metaData = runPut $ makePut key value
    let msg = toStrict $ LS.concat [metaData, key, value]
    res <- send sock msg
    ret <- recv sock 1
    let retCode = BG.runGet getRetCode (toLazy ret)
    return $ retCode == 0

getValue :: Socket -> LS.ByteString -> IO (Maybe LS.ByteString)
getValue sock key = do
    let metaData = runPut $ makeGet key
    let msg = toStrict $ LS.concat [metaData, key]
    res <- send sock msg
    fetch <- recv sock 5
    let (code, valLen) = BG.runGet getGet $ toLazy fetch
    case code of
        0 -> do
            rawValue <- recv sock valLen
            return $ Just (toLazy rawValue)
        _ -> return Nothing

putKeep :: Socket -> LS.ByteString -> LS.ByteString -> IO Bool
putKeep sock key value = do
    let metaData = runPut $ makePutKeep key value
    let msg = toStrict $ LS.concat [metaData, key, value]
    res <- send sock msg
    ret <- recv sock 1
    let retCode = BG.runGet getRetCode (toLazy ret)
    return $ retCode == 0

putCat :: Socket -> LS.ByteString -> LS.ByteString -> IO Bool
putCat sock key value = do
    let metaData = runPut $ makePutCat key value
    let msg = toStrict $ LS.concat [metaData, key, value]
    sent <- send sock msg
    rawRetCode <- recv sock 1
    let retCode = BG.runGet getRetCode (toLazy rawRetCode)
    return $ retCode == 0

sockSuccess :: Socket -> IO Bool
sockSuccess sock = do
    rawRetCode <- recv sock 1
    let retCode = BG.runGet getRetCode (toLazy rawRetCode)
    return $ retCode == 0

out :: Socket -> LS.ByteString -> IO Bool
out sock key = do
    let metaData = runPut $ makeOut key
    let msg = toStrict $ LS.concat [metaData, key]
    sent <- send sock msg
    sockSuccess sock

vsiz :: Socket -> LS.ByteString -> IO (Maybe Int)
vsiz sock key = do
    let metaData = runPut $ makeVsiz key
    let msg = toStrict $ LS.concat [metaData, key]
    res <- send sock msg
    fetch <- recv sock 5
    let (code, valLen) = BG.runGet getGet $ toLazy fetch
    case code of
        0 -> return $ Just valLen
        _ -> return Nothing

--mget :: Socket -> LS.ByteString -> IO [(LS.ByteString, LS.ByteString)]
mget sock keys = do
    let msg = toStrict . runPut $ makeMGet keys
    res <- send sock msg
    hdr <- recv sock 5
    let (code, rnum) = BG.runGet getGet $ toLazy hdr
    pairs <- getManyMGet sock rnum []
    return pairs

makeMGet keys = do
    put C.magic
    put C.mget
    let nkeys = len32 keys
    put nkeys
    let z = [(length32 x, x) | x <- keys]
    mapM_ (\(x, y) -> put x >> putLazyByteString y) z

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

vanish sock = do
    let msg = toStrict . runPut $ (put C.magic >> put C.vanish)
    sent <- send sock msg
    sockSuccess sock

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
    print redid
    --Now slap another "blab" on the end
    catd <- putCat s k v
    print catd
    --show
    hey <- getValue s k
    print hey
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
    areTheyGone <- vanish s
    print areTheyGone
    sClose s
    return pairs
