module Arrow

using Mmap
using Tables, LazyArrays, FillArrays

# TODO remove this when no longer being used
using Debugger

using Base: @propagate_inbounds


const ALIGNMENT = 8

const FILE_FORMAT_MAGIC_BYTES = b"ARROW1"

const BufferOrIO = Union{IO,AbstractVector{UInt8}}


abstract type ArrowVector{T} <: AbstractVector{T} end


struct Unspecified end
const unspecified = Unspecified()

const DefaultOffset = Int32

module Types
    const Values = Any
    const Nullable{T} = Union{T,Missing}
    const List{T} = AbstractVector{T}
    const Null = Missing
    const Strings = AbstractString
end

components(::Type{<:Types.Values}) = (values,)
components(::Type{Types.Nullable{T}}) where {T} = (bitmask, values)
components(::Type{<:Types.List}) = (offsets, values)
components(::Type{<:Types.Null}) = tuple()
components(::Type{<:Types.Strings}) = (offsets, values)

components(::Type{Type}, ::Type{<:AbstractVector{T}}) where {T} = components(T)
components(::AbstractVector{T}) where {T} = components(T)


# TODO ideally views of arrow views would be other arrow views, and not SubArray

function Base.getindex(p::ArrowVector{Union{T,Missing}}, i::Integer) where {T}
    bitmask(p)[i] ? @inbounds(values(p)[i]) : missing
end

function Base.setindex!(p::ArrowVector{Union{T,Missing}}, ::Missing, i::Integer) where {T}
    bitmask(p)[i] = false
    missing
end
function Base.setindex!(p::ArrowVector{Union{T,Missing}}, v, i::Integer) where {T}
    bitmask(p)[i] = true  # bounds checking done here only
    @inbounds values(p)[i] = convert(T, v)
    v
end

include("metadata/Metadata.jl")
using .Metadata; const Meta = Metadata


#============================================================================================
    \start{BitVector}

    NOTE: this shadows a name from `Base`, but it's rarely enough use that we'll just
    live with it.
============================================================================================#
struct BitVector{V<:AbstractVector{UInt8}} <: ArrowVector{Bool}
    values::V
    ℓ::Int
end

values(v::BitVector) = v.values

Base.size(p::BitVector) = (p.ℓ,)
function Base.getindex(p::BitVector, i::Integer)
    @boundscheck checkbounds(p, i)
    @inbounds getbit(values(p), i)
end
function Base.setindex!(p::BitVector, v, i::Integer)
    @boundscheck checkbounds(p, i)
    @inbounds setbit!(values(p), convert(Bool, v), i)
    v
end
#============================================================================================
    \end{BitVector}
============================================================================================#


include("utils.jl")
include("primitives.jl")
include("components.jl")
include("wrappers.jl")
include("deserialization.jl")
include("serialization.jl")
include("file.jl")


# these should more or less already be of this form
compose(v::ArrowVector) = v

compose(v::AbstractVector) = v
compose(v::AbstractVector{Types.Nullable{T}}) where {T} = NullableVector(bitmask(v),
                                                                         compose(values(v)))
compose(v::AbstractVector{<:Types.List}) = List(offsets(v), compose(values(v)))


end  # module Arrow
