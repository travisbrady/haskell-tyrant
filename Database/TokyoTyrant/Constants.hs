module Database.TokyoTyrant.Constants where

import Data.Word (Word8)
import Data.Bits (shiftL)
import Data.Int

magic :: Word8
magic = 0xc8
put :: Word8
put = 0x10
putkeep :: Word8
putkeep = 0x11
putcat :: Word8
putcat = 0x12
putshl :: Word8 
putshl = 0x13
putnr :: Word8
putnr = 0x18
out :: Word8
out = 0x20
get :: Word8
get = 0x30
mget :: Word8
mget = 0x31
vsiz :: Word8
vsiz = 0x38
iterinit :: Word8
iterinit = 0x50
iternext :: Word8
iternext = 0x51
fwmkeys :: Word8
fwmkeys = 0x58
addint :: Word8
addint = 0x60
adddouble :: Word8
adddouble = 0x61
ext :: Word8
ext = 0x68
sync :: Word8
sync = 0x70
vanish :: Word8
vanish = 0x71
copy :: Word8
copy = 0x72
restore :: Word8
restore = 0x73
setmst :: Word8
setmst = 0x78
rnum :: Word8
rnum = 0x80
size :: Word8
size = 0x81
stat :: Word8
stat = 0x88
misc :: Word8
misc = 0x90

rDBMONOULOG :: Int32
rDBMONOULOG = 1 `shiftL` 0  -- No update log
rDBXOLCKREC  :: Int32
rDBXOLCKREC = 1 `shiftL` 0  -- Record locking
rDBXOLCKGLB :: Int32
rDBXOLCKGLB = 1 `shiftL` 1  -- Global locking
