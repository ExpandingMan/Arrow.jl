module Arrow

using Mmap
using Tables, LazyArrays, FillArrays

# TODO remove this when no longer being used
using Debugger

using Base: @propagate_inbounds


const ALIGNMENT = 8

const FILE_FORMAT_MAGIC_BYTES = b"ARROW1"

const CONTINUATION_INDICATOR_BYTES = 0xffffffff

const BufferOrIO = Union{IO,AbstractVector{UInt8}}
const ColumnName = Union{AbstractString,Symbol}


abstract type ArrowVector{T} <: AbstractVector{T} end


"""
    Unspecified

A singleton type, the instance of which is `unspecified`.  See `unspecified`.
"""
struct Unspecified end

"""
    unspecified

A singleton of type `Unspecified` which is used in cases where the arrow standard does not specify
a value.

Mostly this is used for writing the underlying values of arrays with `missing` elements.
"""
const unspecified = Unspecified()


const DefaultOffset = Int32


"""
    Types

A module containing type aliases corresponding to the appropriate arrow array wrappers for a given
`AbstractVector` element type.
"""
module Types
    const Values = Any
    const Nullable{T} = Union{T,Missing}
    const List{T} = AbstractVector{T}
    const Null = Missing
    const Strings = AbstractString
end

"""
    components(T::Type)
    components(v::AbstractVector)

Returns a tuple the elements which are functions for returning the (highest level) arrow components
of the array `v`, or array with element type `T`.
"""
components(::Type{<:Types.Values}) = (values,)
components(::Type{Types.Nullable{T}}) where {T} = (bitmask, values)
components(::Type{<:Types.List}) = (offsets, values)
components(::Type{<:Types.Null}) = tuple()
components(::Type{<:Types.Strings}) = (offsets, values)

components(::Type{Type}, ::Type{<:AbstractVector{T}}) where {T} = components(T)
components(::Type{Function}, ::AbstractVector{T}) where {T} = components(T)


struct ComponentIterator{V<:AbstractVector}
    array::V
end

Base.IteratorSize(::ComponentIterator) = Base.SizeUnknown()

components(v::AbstractVector) = ComponentIterator(v)

components(::Type{Function}, ci::ComponentIterator) = components(Function, ci.array)

function Base.iterate(ci::ComponentIterator, o=ci.array)
    cs = components(Function, o)
    isempty(cs) && return nothing
    o1 = first(cs)(o)
    if length(cs) == 1
        (first(cs), o1), nothing
    else
        (first(cs), o1), cs[2](o)
    end
end
Base.iterate(ci::ComponentIterator, ::Nothing) = nothing


Base.IndexStyle(::Type{<:ArrowVector}) = IndexLinear()

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

# TODO should define `view` to return another arrow view where appropriate, rather than a SubArray

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


"""
    compose(v::AbstractVector)

Create `AbstractVector` with an analogous hierarchical structure to the Arrow format of `v`.
This is a lazy operation that does not create any buffers, however the resulting object is appropriate
for creating arrow buffers.
"""
compose(v::ArrowVector) = v

# TODO need to do strings and stuff
compose(v::AbstractVector) = v
compose(v::AbstractVector{Types.Nullable{T}}) where {T} = NullableVector(bitmask(v),
                                                                         compose(values(v)))
compose(v::AbstractVector{<:Types.List}) = List(offsets(v), compose(values(v)))
compose(v::AbstractVector{<:Types.Strings}) = ConvertVector{String}(compose(codeunits.(v)))


end  # module Arrow
