# endian.nim - Fast, target-agnostic Big-Endian and Little-Endian codecs
# Licensed under MIT License.

func putBe16*(buf: var openArray[byte], offset: int, val: uint16) {.inline.} =
  buf[offset + 0] = byte((val shr 8) and 0xff'u16)
  buf[offset + 1] = byte(val and 0xff'u16)

func getBe16*(buf: openArray[byte], offset: int): uint16 {.inline.} =
  (uint16(buf[offset + 0]) shl 8) or uint16(buf[offset + 1])

func putLe16*(buf: var openArray[byte], offset: int, val: uint16) {.inline.} =
  buf[offset + 0] = byte(val and 0xff'u16)
  buf[offset + 1] = byte((val shr 8) and 0xff'u16)

func getLe16*(buf: openArray[byte], offset: int): uint16 {.inline.} =
  uint16(buf[offset + 0]) or (uint16(buf[offset + 1]) shl 8)

func putBe32*(buf: var openArray[byte], offset: int, val: uint32) {.inline.} =
  buf[offset + 0] = byte((val shr 24) and 0xff'u32)
  buf[offset + 1] = byte((val shr 16) and 0xff'u32)
  buf[offset + 2] = byte((val shr 8) and 0xff'u32)
  buf[offset + 3] = byte(val and 0xff'u32)

func getBe32*(buf: openArray[byte], offset: int): uint32 {.inline.} =
  (uint32(buf[offset + 0]) shl 24) or (uint32(buf[offset + 1]) shl 16) or
    (uint32(buf[offset + 2]) shl 8) or uint32(buf[offset + 3])

func putLe32*(buf: var openArray[byte], offset: int, val: uint32) {.inline.} =
  buf[offset + 0] = byte(val and 0xff'u32)
  buf[offset + 1] = byte((val shr 8) and 0xff'u32)
  buf[offset + 2] = byte((val shr 16) and 0xff'u32)
  buf[offset + 3] = byte((val shr 24) and 0xff'u32)

func getLe32*(buf: openArray[byte], offset: int): uint32 {.inline.} =
  uint32(buf[offset + 0]) or (uint32(buf[offset + 1]) shl 8) or
    (uint32(buf[offset + 2]) shl 16) or (uint32(buf[offset + 3]) shl 24)

func putBe64*(buf: var openArray[byte], offset: int, val: uint64) {.inline.} =
  buf[offset + 0] = byte((val shr 56) and 0xff'u64)
  buf[offset + 1] = byte((val shr 48) and 0xff'u64)
  buf[offset + 2] = byte((val shr 40) and 0xff'u64)
  buf[offset + 3] = byte((val shr 32) and 0xff'u64)
  buf[offset + 4] = byte((val shr 24) and 0xff'u64)
  buf[offset + 5] = byte((val shr 16) and 0xff'u64)
  buf[offset + 6] = byte((val shr 8) and 0xff'u64)
  buf[offset + 7] = byte(val and 0xff'u64)

func getBe64*(buf: openArray[byte], offset: int): uint64 {.inline.} =
  (uint64(buf[offset + 0]) shl 56) or (uint64(buf[offset + 1]) shl 48) or
    (uint64(buf[offset + 2]) shl 40) or (uint64(buf[offset + 3]) shl 32) or
    (uint64(buf[offset + 4]) shl 24) or (uint64(buf[offset + 5]) shl 16) or
    (uint64(buf[offset + 6]) shl 8) or uint64(buf[offset + 7])
