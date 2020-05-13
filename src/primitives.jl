
abstract type AbstractPrimitive{T} <: ArrowVector{T} end

#============================================================================================
    \begin{Primitive}

    NOTE: Ideally these would be replaced by some sort of reinterpret view from base
    but so far can't quite get the performance out of those that we want.
============================================================================================#
struct Primitive{T} <: AbstractPrimitive{T}
    buffer::Vector{UInt8}
    idx::Int
    ℓ::Int
end

Base.size(p::Primitive) = (p.ℓ,)

start_idx(p::Primitive) = p.idx
@propagate_inbounds function start_idx(p::Primitive, i::Integer)
    @boundscheck checkbounds(p, i)
    start_idx(p) + (i-1)*sizeof(eltype(p))
end
@propagate_inbounds function end_idx(p::Primitive, i::Integer)
    @boundscheck checkbounds(p, i)
    start_idx(p) + i*sizeof(eltype(p)) - 1
end
end_idx(p::Primitive) = end_idx(p, length(p))

buffer_indices(p::Primitive, i::Integer) = (start_idx(p, i), end_idx(p, i))
buffer_indices(p::Primitive) = (start_idx(p), end_idx(p))

bufferpointer(p::Primitive) = pointer(p.buffer)

valuespointer(p::Primitive)  = bufferpointer(p) + start_idx(p) - 1
@propagate_inbounds function valuespointer(p::Primitive, i::Integer)
    bufferpointer(p) + start_idx(p, i) - 1
end

@propagate_inbounds function getvalue(p::Primitive{T}, i::Integer) where {T}
    first(reinterpret(T, view(p.buffer, start_idx(p, i), @inbounds end_idx(p, i))))
end

@propagate_inbounds function unsafe_getvalue(p::Primitive{T}, i::Integer) where {T}
    unsafe_load(convert(Ptr{T}, valuespointer(p, i)))
end

Base.getindex(p::Primitive, i::Integer) = unsafe_getvalue(p, i)

bytecount(p::Primitive) = length(p)*sizeof(eltype(p))
bitcount(p::Primitive) = 8bytecount(p)

@propagate_inbounds getbyte(p::Primitive, i::Integer) = p.buffer[start_idx(p) + i - 1]
@propagate_inbounds function getbit(p::Primitive, i::Integer)
    a, b = fldmod1(i, 8)
    _getbit(getbyte(p, a), b)
end

@propagate_inbounds function setbyte!(p::Primitive, b::UInt8, i::Integer)
    p.buffer[start_idx(p) + i - 1] = b
end
@propagate_inbounds function setbit!(p::Primitive, v::Bool, i::Integer)
    a, b = fldmod1(i, 8)
    setbyte!(p, _setbit(getbyte(p, a), v, b), a)
end

# TODO: this is unbelievably slow right now and has allocations
function Base.setindex!(p::Primitive{T}, v, i::Integer) where {T}
    p.buffer[start_idx(p, i):end_idx(p, i)] = reinterpret(UInt8, [convert(T, v)])
    v
end

values(p::Primitive) = p
#============================================================================================
    \end{Primitive}
============================================================================================#
