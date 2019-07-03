
"""
    padding(n::Integer)

Determines the total number of bytes needed to store `n` bytes with padding.
Note that the Arrow standard requires buffers to be aligned to 8-byte boundaries.
"""
padding(n::Integer) = ((n + ALIGNMENT - 1) ÷ ALIGNMENT)*ALIGNMENT

paddinglength(n::Integer) = padding(n) - n


"""
    writepadded(io::IO, x)

Write the data `x` to `io` with 8-byte padding. This is commonly needed in Arrow implementations
since Arrow requires 8-byte boundary alignment.
"""
function writepadded(io::IO, x)
    bw = write(io, x)
    diff = padding(bw) - bw
    write(io, zeros(UInt8, diff))
    bw + diff
end


"""
    _getbit

This deliberately elides bounds checking.
"""
_getbit(v::UInt8, n::Integer) = Bool((v & 0x02^(n-1)) >> (n-1))

"""
    _setbit

This also deliberately elides bounds checking.
"""
function _setbit(v::UInt8, b::Bool, n::Integer)
    if b
        v | 0x02^(n-1)
    else
        v & (0xff ⊻ 0x02^(n-1))
    end
end


function _bitpack_byte(a::AbstractVector{Bool}, nbits::Integer)
    o = 0x00
    for i ∈ 1:nbits
        o += UInt8(a[i]) << (i-1)
    end
    o
end

function _bitpackedbytes(n::Integer, pad::Bool=true)
    a, b = divrem(n, 8)
    ℓ = a + (b > 0)
    pad && (ℓ += paddinglength(ℓ))
    ℓ, a, b
end
function bitpackedbytes(n::Integer, pad::Bool=true)
    ℓ, a, b = _bitpackedbytes(n, pad)
    ℓ
end

"""
    bitpack!(buffer, A::AbstractVector{Bool}, idx=1)

Pack a vector of `Bool`s into bits and place in `buffer` starting at index `idx`.
"""
function bitpack!(buffer::Vector{UInt8}, A::AbstractVector{Bool}, idx::Integer=1,
                  (a, b)::Tuple=divrem(length(A), 8))
    for i ∈ 1:a
        k = (i-1)*8 + 1
        buffer[idx + i - 1] = _bitpack_byte(view(A, k:(k+7)), 8)
    end
    if b > 0
        trail = (a*8+1):length(A)
        buffer[idx + a] = _bitpack_byte(view(A, trail), length(trail))
    end
    buffer
end

function bitpack(A::AbstractVector{Bool}, buff_idx::Integer=1, pad::Bool=true,
                 (ℓ, a, b)::Tuple=_bitpackedbytes(length(A), pad))
    bitpack!(zeros(UInt8, ℓ), A, buff_idx, (a, b))
end

function bitpackpadded(A::AbstractVector{Bool})
    v = bitpack(A)
    npad = paddinglength(length(v))
    vcat(v, zeros(UInt8, npad))
end


"""
    bare_eltype(v)

Gets the eltype of an `AbstractVector`, except that if this is of the form `Union{T,Missing}`,
return `T` instead.
"""
bare_eltype(::AbstractVector{T}) where {T} = T
bare_eltype(::AbstractVector{Union{T,Missing}}) where{T} = T
