-- A few basic tests
module Main where

import qualified Data.ByteString.Lazy.Char8 as LS
import Database.TokyoTyrant

-- Call the fibonacci function used in the Tokyo Tyrant tutorial at:
-- http://tokyocabinet.sourceforge.net/tyrantdoc/#tutorial_luaext
testExt conn = do
    let v = LS.pack "9"
    let fname = LS.pack "fibonacci"
    ret <- ext conn fname v LS.empty []
    return ret

-- fetch a few values at once
testMget conn = do
    let k1 = LS.pack "CA"
    let v1 = LS.pack "California"
    let k2 = LS.pack "OR"
    let v2 = LS.pack "Oregon"
    putValue conn k1 v1
    putValue conn k2 v2
    res <- mget conn [k1, k2]
    print res

testBasic conn = do
    let k = LS.pack "k1"
    let v = LS.pack "v1"
    success <- putValue conn k v
    g <- getValue conn k
    let v2 = LS.pack "v2"
    pkp <- putKeep conn k v2
    g2 <- getValue conn k
    print (g, g2)
    print $ g == g2
    ptct <- putCat conn k v2
    rawg3 <- getValue conn k
    let (Right g3) = rawg3
    let shouldBe = LS.concat [v, v2]
    print $ g3 == shouldBe
    valSize <- vsiz conn k
    print valSize
    let k3 = LS.pack "sub"
    let v3 = LS.pack "v3"
    blub <- putshl conn k3 v3 10
    let k4 = LS.pack "k4"
    let v4 = LS.pack "v4"
    pnrd <- putnr conn k4 v4
    killed <- out conn k
    print killed

testputDouble conn = do
    pd <- putDouble conn (LS.pack "k1") 999.9
    print pd

-- Put and get a 128 kB string. Will require multiple send/recv operations when
-- going to a server on a remote host.
testLarge conn = do
  let bigStr = LS.pack $ replicate (1024*128) '@'
  ret <- putValue conn (LS.pack "bigStr") bigStr
  print ret
  gval <- getValue conn (LS.pack "bigStr")
  print $ gval == (Right bigStr)

main = do
    s <- openConnection "localhost" "1978"
    let outPath = LS.pack "hogo.tch"
    copyConfirm <- copy s outPath
    print copyConfirm
    let k3 = LS.pack "dude"
    jungle <- addInt s k3 (20::Int)
    print jungle
    sz <- size s
    print sz
    numrecs <- rnum s
    print numrecs
    stats <- stat s
    print stats
    dubx <- addDouble s (LS.pack "k5") 5.5
    let k6 = LS.pack "blah"
    dud <- addDouble s k6 10.005
    theDub <- getDouble s k6
    print theDub
    let k7 = LS.pack "k7"
    ai <- addInt s k7 1
    theInt <- getInt s k7
    print theInt
    let k10 = LS.pack "k10"
    let v10 = LS.pack "v10"
    blub <- putshl s k10 v10 10
    let k11 = LS.pack "k11"
    let v11 = LS.pack "v11"
    pnrd <- putnr s k11 v11
    gog <- iterinit s
    ham <- iternext s
    sam <- iternext s
    print (gog, ham, sam)
    let k12 = LS.pack "k12"
    let v12 = LS.pack "v11"
    blump <- putValue s k12 v12
    fmks <- fwmkeys s (LS.pack "k") 10
    print fmks
    misky <- misc s (LS.pack "getlist") [LS.pack "k12"] []
    let keyList = map LS.pack ["k14", "v14", "k15", "v15"]
    masky <- misc s (LS.pack "putlist") keyList []
    testBasic s
    testExt s
    testMget s
    areTheyGone <- vanish s
    print areTheyGone
    testputDouble s
    testLarge s
    closeConnection s
