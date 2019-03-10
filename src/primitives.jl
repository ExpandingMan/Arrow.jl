
abstract type AbstractPrimitive{T} <: ArrowVector{T} end

#============================================================================================
    \begin{Primitive}
============================================================================================#
struct Primitive{T} <: AbstractPrimitive{T}
    buffer::Vector{UInt8}
    idx::Int
    ℓ::Int
end

# TODO need to check how much of this can be re-absorbed into ArrowVector
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
#============================================================================================
    \end{Primitive}
============================================================================================#

