{-# LANGUAGE OverloadedStrings #-}
-- A few basic tests. Run the server with:
--
--   $ ttserver -ext test.lua
--
module Main where

import qualified Data.ByteString.Lazy.Char8 as LS
import Database.TokyoTyrant
import Test.HUnit
import Data.List (sort)

-- Call the fibonacci function used in the Tokyo Tyrant tutorial at:
-- http://tokyocabinet.sourceforge.net/tyrantdoc/#tutorial_luaext
testExt conn = do
    let v = "9"
    let fname = "fibonacci"
    ret <- ext conn fname v LS.empty []
    ret @?= Right "34"

-- fetch a few values at once
testMget conn = do
    let k1 = "CA"
    let v1 = "California"
    let k2 = "OR"
    let v2 = "Oregon"
    putValue conn k1 v1
    putValue conn k2 v2
    res <- mget conn [k1, k2]
    res @?= Right [(k2, v2), (k1, v1)]

testBasic conn = do
    let k = "k1"
    let v = "v1"
    success <- putValue conn k v
    g <- getValue conn k
    let v2 = "v2"
    pkp <- putKeep conn k v2
    g2 <- getValue conn k
    g @?= g2
    ptct <- putCat conn k v2
    rawg3 <- getValue conn k
    let (Right g3) = rawg3
    let shouldBe = LS.concat [v, v2]
    g3 @?= shouldBe
    valSize <- vsiz conn k
    valSize @?= Right 4
    let k3 = "sub"
    let v3 = "v3"
    blub <- putshl conn k3 v3 10
    let k4 = "k4"
    let v4 = "v4"
    pnrd <- putnr conn k4 v4
    killed <- out conn k
    killed @?= Right "success"

testputDouble conn = do
    pd <- putDouble conn ("k1") 42.5
    pd @?= Right 42.5

-- Put and get a 128 kB string. Will require multiple send/recv operations when
-- going to a server on a remote host.
testLarge conn = do
  let bigStr = LS.replicate (1024*128) '@'
  ret <- putValue conn "bigStr" bigStr
  gval <- getValue conn "bigStr"
  gval @?= Right bigStr

mainTest = do
    s <- openConnection "localhost" "1978"
    let outPath = "hogo.tch"
    copyConfirm <- copy s outPath
    copyConfirm @?= Right "success"
    let k3 = "dude"
    jungle <- addInt s k3 (20::Int)
    jungle @?= Right 20
    sz <- size s
    sz @?= Right 656598
    numrecs <- rnum s
    numrecs @?= Right 3
    Right stats <- stat s
    length stats > 0 @?= True    
    dubx <- addDouble s ("k5") 5.5
    let k6 = "blah"
    dud <- addDouble s k6 10.005
    theDub <- getDouble s k6
    theDub @?= Right 10.005
    let k7 = "k7"
    ai <- addInt s k7 1
    theInt <- getInt s k7
    theInt @?= Right 1
    let k10 = "k10"
    let v10 = "v10"
    blub <- putshl s k10 v10 10
    let k11 = "k11"
    let v11 = "v11"
    pnrd <- putnr s k11 v11
    gog <- iterinit s
    ham <- iternext s
    sam <- iternext s
    gog @?= Right "success"
    ham @?= Right "k11"
    sam @?= Right "blah"
    let k12 = "k12"
    let v12 = "v11"
    blump <- putValue s k12 v12
    Right fmks <- fwmkeys s ("k") 10
    sort fmks @?= sort ["k10", "k1", "k7", "k5", "k12", "k11"]
    misky <- misc s ("getlist") ["k12"] []
    let keyList = ["k14", "v14", "k15", "v15"]
    masky <- misc s ("putlist") keyList []
    testBasic s
    testExt s
    testMget s
    areTheyGone <- vanish s
    areTheyGone @?= Right "success"
    testputDouble s
    testLarge s
    closeConnection s

main = runTestTT $ TestCase mainTest
