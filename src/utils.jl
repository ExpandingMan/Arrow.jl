
"""
    padding(n::Integer)

Determines the total number of bytes needed to store `n` bytes with padding.
Note that the Arrow standard requires buffers to be aligned to 8-byte boundaries.
"""
padding(n::Integer) = ((n + ALIGNMENT - 1) ÷ ALIGNMENT)*ALIGNMENT


paddinglength(n::Integer) = padding(n) - n


"""
    writepadded(io::IO, x)
    writepadded(io::IO, A::Primitive)
    writepadded(io::IO, A::Arrowvector, subbuffs::Function...)

Write the data `x` to `io` with 8-byte padding. This is commonly needed in Arrow implementations
since Arrow requires 8-byte boundary alignment.

If a `Primitive` is provided, the appropriate padded values will be written.

If an `ArrowVector` is provided, the ordering of the sub-buffers must be specified, and they will
be written in the order given.  For example `writepadded(io, A, bitmask, offsets, values)` will write
the bit mask, offsets and then values of `A`.
"""
function writepadded(io::IO, x)
    bw = write(io, x)
    diff = padding(bw) - bw
    write(io, zeros(UInt8, diff))
    bw + diff
end


"""
    _getbit

This deliberate elides bounds checking.
"""
_getbit(v::UInt8, n::Integer) = Bool((v & 0x02^(n-1)) >> (n-1))
