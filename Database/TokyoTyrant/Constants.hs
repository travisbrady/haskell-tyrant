module Database.TokyoTyrant.Constants where

import Data.Word (Word8)
import Data.Bits (shiftL)
import Data.Int

magic     = 0xc8 :: Word8
put       = 0x10 :: Word8
putkeep   = 0x11 :: Word8
putcat    = 0x12 :: Word8
putshl    = 0x13 :: Word8
putnr     = 0x18 :: Word8
out       = 0x20 :: Word8
get       = 0x30 :: Word8
mget      = 0x31 :: Word8
vsiz      = 0x38 :: Word8
iterinit  = 0x50 :: Word8
iternext  = 0x51 :: Word8
fwmkeys   = 0x58 :: Word8
addint    = 0x60 :: Word8
adddouble = 0x61 :: Word8
ext       = 0x68 :: Word8
sync      = 0x70 :: Word8
vanish    = 0x71 :: Word8
copy      = 0x72 :: Word8
restore   = 0x73 :: Word8
setmst    = 0x78 :: Word8
rnum      = 0x80 :: Word8
size      = 0x81 :: Word8
stat      = 0x88 :: Word8
misc      = 0x90 :: Word8

rDBMONOULOG :: Int32
rDBMONOULOG = 1 `shiftL` 0  -- No update log
rDBXOLCKREC  :: Int32
rDBXOLCKREC = 1 `shiftL` 0  -- Record locking
rDBXOLCKGLB :: Int32
rDBXOLCKGLB = 1 `shiftL` 1  -- Global locking
