## Can be shared safely between threads
type SharedSeq*[T] = tuple[data: ptr UncheckedArray[T], len: int]

proc alloc*(str: cstring): cstring =
  # Byte allocation from the given address.
  # There should be the corresponding manual deallocation with deallocShared !
  if str.isNil():
    var ret = cast[cstring](allocShared(1)) # Allocate memory for the null terminator
    ret[0] = '\0' # Set the null terminator
    return ret

  let ret = cast[cstring](allocShared(len(str) + 1))
  copyMem(ret, str, len(str) + 1)
  return ret

proc alloc*(str: string): cstring =
  ## Byte allocation from the given address.
  ## There should be the corresponding manual deallocation with deallocShared !
  var ret = cast[cstring](allocShared(str.len + 1))
  let s = cast[seq[char]](str)
  for i in 0 ..< str.len:
    ret[i] = s[i]
  ret[str.len] = '\0'
  return ret

proc allocSharedSeq*[T](s: seq[T]): SharedSeq[T] =
  let data = allocShared(sizeof(T) * s.len)
  if s.len != 0:
    copyMem(data, unsafeAddr s[0], s.len)
  return (cast[ptr UncheckedArray[T]](data), s.len)

proc deallocSharedSeq*[T](s: var SharedSeq[T]) =
  deallocShared(s.data)
  s.len = 0

proc toSeq*[T](s: SharedSeq[T]): seq[T] =
  ## Creates a seq[T] from a SharedSeq[T]. No explicit dealloc is required
  ## as req[T] is a GC managed type.
  var ret = newSeq[T]()
  for i in 0 ..< s.len:
    ret.add(s.data[i])
  return ret
