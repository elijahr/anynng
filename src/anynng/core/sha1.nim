# sha1.nim - Pure-Nim RFC 3174 SHA-1 Digest for WebSocket Handshake (Zero Warnings / Zero Deps)
# Licensed under MIT License.

func rol(val: uint32, shift: int): uint32 {.inline.} =
  (val shl shift) or (val shr (32 - shift))

func sha1Digest*(data: openArray[byte]): array[20, byte] =
  ## Computes SHA-1 hash of data according to RFC 3174.
  var h0 = 0x67452301'u32
  var h1 = 0xEFCDAB89'u32
  var h2 = 0x98BADCFE'u32
  var h3 = 0x10325476'u32
  var h4 = 0xC3D2E1F0'u32

  let bitLen = uint64(data.len) * 8'u64
  var msg = newSeq[byte](data.len)
  if data.len > 0:
    copyMem(addr msg[0], unsafeAddr data[0], data.len)
  msg.add(0x80'u8)
  while (msg.len mod 64) != 56:
    msg.add(0x00'u8)

  for i in countdown(7, 0):
    msg.add(byte((bitLen shr (i * 8)) and 0xff'u64))

  var w: array[80, uint32]
  let numChunks = msg.len div 64
  for chunkIdx in 0 ..< numChunks:
    let offset = chunkIdx * 64
    for i in 0 ..< 16:
      w[i] =
        (uint32(msg[offset + i * 4]) shl 24) or (uint32(msg[offset + i * 4 + 1]) shl 16) or
        (uint32(msg[offset + i * 4 + 2]) shl 8) or (uint32(msg[offset + i * 4 + 3]))
    for i in 16 ..< 80:
      w[i] = rol(w[i - 3] xor w[i - 8] xor w[i - 14] xor w[i - 16], 1)

    var a = h0
    var b = h1
    var c = h2
    var d = h3
    var e = h4

    for i in 0 ..< 80:
      var f: uint32
      var k: uint32
      if i < 20:
        f = (b and c) or ((not b) and d)
        k = 0x5A827999'u32
      elif i < 40:
        f = b xor c xor d
        k = 0x6ED9EBA1'u32
      elif i < 60:
        f = (b and c) or (b and d) or (c and d)
        k = 0x8F1BBCDC'u32
      else:
        f = b xor c xor d
        k = 0xCA62C1D6'u32

      let temp = rol(a, 5) + f + e + k + w[i]
      e = d
      d = c
      c = rol(b, 30)
      b = a
      a = temp

    h0 = h0 + a
    h1 = h1 + b
    h2 = h2 + c
    h3 = h3 + d
    h4 = h4 + e

  let h = [h0, h1, h2, h3, h4]
  for i in 0 ..< 5:
    result[i * 4] = byte((h[i] shr 24) and 0xff'u32)
    result[i * 4 + 1] = byte((h[i] shr 16) and 0xff'u32)
    result[i * 4 + 2] = byte((h[i] shr 8) and 0xff'u32)
    result[i * 4 + 3] = byte(h[i] and 0xff'u32)
