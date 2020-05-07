
#============================================================================================
    \begin{Primitive Constructors}
============================================================================================#
function Primitive!(buf::Vector{UInt8}, v::AbstractVector, i::Integer=1)
    serialize!(view(buf, i:lastindex(buf)), Primitive, v)
    Primitive{eltype(v)}(buf, i, length(v))
end
function Primitive(v::AbstractVector, i::Integer=1)
    Primitive!(Vector{UInt8}(undef, length(v)*sizeof(eltype(v))), v, i)
end
#============================================================================================
    \end{Primitive Constructors}
============================================================================================#

#============================================================================================
    \begin{BitPrimitive Constructors}
============================================================================================#
function BitPrimitive!(buf::Vector{UInt8}, v::AbstractVector{Bool}, i::Integer=1)
    serialize!(view(buf, i:lastindex(buf)), BitPrimitive, v)
    BitPrimitive(Primitive{UInt8}(buf, i, bitpackedbytes(length(v), false)), length(v))
end
function BitPrimitive(v::AbstractVector{Bool})
    BitPrimitive!(Vector{UInt8}(undef, bitpackedbytes(length(v))), v)
end
#============================================================================================
    \end{BitPrimitive Constructors}
============================================================================================#

#============================================================================================
    \begin{bitmasks}
============================================================================================#
bitmaskbytes(n::Integer; pad::Bool=true) = bitpackedbytes(n, pad)
bitmaskbytes(v::AbstractVector; pad::Bool=true) = bitmaskbytes(length(v), pad=pad)

function bitmask!(buf::Vector{UInt8}, v::AbstractVector, i::Integer=1)
    serialize!(view(buf, i:lastindex(buf)), bitmask, v)
    BitPrimitive(Primitive{UInt8}(buf, i, bitpackedbytes(length(v), false)), length(v))
end
function bitmask(v::AbstractVector)
    bitmask!(Vector{UInt8}(undef, bitpackedbytes(length(v))), v)
end
#============================================================================================
    \end{bitmasks}
============================================================================================#

#============================================================================================
    \begin{offsets}
============================================================================================#
function offsetsbytes(n::Integer; pad::Bool=true)
    ℓ = (n+1)*sizeof(DefaultOffset)
    pad ? padding(ℓ) : ℓ
end
offsetsbytes(v::AbstractVector; pad::Bool=true) = offsetsbytes(length(v), pad=pad)

"""
    offlength(v)

Compute the length of the object `v` for the purposes of determining offsets.  This defaults to
`length`, but has special handling for some cases like `AbstractString`.
"""
offlength(v) = length(v)
offlength(s::AbstractString) = ncodeunits(s)

# NOTE: the eltype constraint ensures that the size computation is correct
function offsets!(off::AbstractVector{DefaultOffset}, v::AbstractVector{T},
                  i::Integer=1) where {T<:Union{Vector,String}}
    off[i] = 0
    for j ∈ 2:(length(v)+1)
        off[i+j-1] = offlength(v[j-1]) + off[j-1]
    end
    off
end
function offsets!(buf::Vector{UInt8}, v::AbstractVector{T},
                  i::Integer=1) where {T<:Union{Vector,String}}
    serialize!(view(buf, i:lastindex(buf)), offsets, v)
    Primitive{DefaultOffset}(buf, i, length(v)+1)
end
offsets(v::AbstractVector) = offsets!(Vector{UInt8}(undef, offsetsbytes(v)), v)
#============================================================================================
    \end{offsets}
============================================================================================#

#============================================================================================
    \begin{from RecordBatch}
============================================================================================#
function primitive(::Type{T}, b::Meta.Buffer, buf::Vector{UInt8}, ℓ::Integer, i::Integer=1
                  ) where {T}
    Primitive{T}(buf, i + b.offset, ℓ)
end
function primitive(::Type{T}, ϕn::Meta.FieldNode, b::Meta.Buffer, buf::Vector{UInt8},
                   i::Integer=1) where {T}
    primitive(T, b, buf, ϕn.length, i)
end
function primitive(::Type{T}, rb::Meta.RecordBatch, buf::Vector{UInt8},
                   node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1) where {T}
    primitive(T, rb.nodes[node_idx], rb.buffers[buf_idx], buf, i)
end

function bitprimitive(ϕn::Meta.FieldNode, b::Meta.Buffer, buf::Vector{UInt8}, i::Integer=1)
    BitPrimitive(primitive(UInt8, ϕn, b, buf, i), ϕn.length)
end
function bitprimitive(rb::Meta.RecordBatch, buf::Vector{UInt8},
                      node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    bitprimitive(rb.nodes[node_idx], rb.buffers[buf_idx], buf, i)
end

function bitmask(ϕn::Meta.FieldNode, b::Meta.Buffer, buf::Vector{UInt8}, i::Integer=1)
    bitprimitive(ϕn, b, buf, i)
end
function bitmask(rb::Meta.RecordBatch, buf::Vector{UInt8},
                 node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    bitmask(rb.nodes[node_idx], rb.buffers[buf_idx], buf, i)
end

function offsets(ϕn::Meta.FieldNode, b::Meta.Buffer, buf::Vector{UInt8}, i::Integer=1)
    Primitive{DefaultOffset}(buf, i + b.offset, ϕn.length+1)
end
function offsets(rb::Meta.RecordBatch, buf::Vector{UInt8},
                 node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    offsets(rb.nodes[node_idx], rb.buffers[buf_idx], buf, i)
end

function _check_empty_buffer(rb::Meta.RecordBatch, node_idx::Integer, buf_idx::Integer)
    rb.nodes[node_idx].null_count == 0 && rb.buffers[buf_idx].length == 0
end

# TODO is the ordering of the sub-buffers canonical???
function build(::Type{AbstractVector{T}}, rb::Meta.RecordBatch, buf::Vector{UInt8},
               node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1) where {T}
    primitive(T, rb, buf, node_idx, buf_idx, i), node_idx+1, buf_idx+1
end
function build(::Type{AbstractVector{Union{T,Missing}}}, rb::Meta.RecordBatch,
               buf::Vector{UInt8}, node_idx::Integer=1, buf_idx::Integer=1,
               i::Integer=1) where {T}
    # handle cases where schema says nullable, but mask is missing
    if _check_empty_buffer(rb, node_idx, buf_idx)
        return build(AbstractVector{T}, rb, buf, node_idx, buf_idx+1, i)
    end
    b = bitmask(rb, buf, node_idx, buf_idx, i)
    buf_idx += 1
    v, node_idx, buf_idx = build(AbstractVector{T}, rb, buf, node_idx, buf_idx, i)
    NullableVector{T,typeof(v)}(v, b), node_idx, buf_idx
end
function build(::Type{AbstractVector{Vector{T}}}, rb::Meta.RecordBatch,
               buf::Vector{UInt8}, node_idx::Integer=1, buf_idx::Integer=1,
               i::Integer=1) where {T}
    o = offsets(rb, buf, node_idx, buf_idx, i)
    node_idx += 1
    buf_idx += 1
    v, node_idx, buf_idx = build(AbstractVector{T}, rb, buf, node_idx, buf_idx, i)
    List{eltype(v),typeof(v)}(v, o), node_idx, buf_idx
end
function build(::Type{AbstractVector{Union{Vector{T},Missing}}}, rb::Meta.RecordBatch,
               buf::Vector{UInt8}, node_idx::Integer=1, buf_idx::Integer=1,
               i::Integer=1) where {T}
    if _check_empty_buffer(rb, node_idx, buf_idx)
        return build(AbstractVector{Vector{T}}, rb, buf, node_idx, buf_idx+1, i)
    end
    b = bitmask(rb, buf, node_idx, buf_idx, i)
    buf_idx += 1
    l, node_idx, buf_idx = build(AbstractVector{Vector{T}}, rb, buf, node_idx, buf_idx, i)
    NullableVector{bare_eltype(l),typeof(l)}(l, b), node_idx, buf_idx
end

function _string_list(rb::Meta.RecordBatch, buf::Vector{UInt8}, node_idx::Integer=1,
                      buf_idx::Integer=1, i::Integer=1)
    o = offsets(rb, buf, node_idx, buf_idx, i)
    buf_idx += 1
    # note that the creation of this primitive also requires special handling
    v = primitive(UInt8, rb.buffers[buf_idx], buf, rb.buffers[buf_idx].length, i)
    List{eltype(v),typeof(v)}(v, o)
end
# NOTE: they left us no choice but to have special methods for strings
function build(::Type{AbstractVector{String}}, rb::Meta.RecordBatch, buf::Vector{UInt8},
               node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    l = _string_list(rb, buf, node_idx, buf_idx, i)
    ConvertVector{String,typeof(l)}(l), node_idx+1, buf_idx+2
end
function build(::Type{AbstractVector{Union{String,Missing}}}, rb::Meta.RecordBatch,
               buf::Vector{UInt8}, node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    if _check_empty_buffer(rb, node_idx, buf_idx)
        return build(AbstractVector{String}, rb, buf, node_idx, buf_idx+1, i)
    end
    b = bitmask(rb, buf, node_idx, buf_idx, i)
    buf_idx += 1
    l = _string_list(rb, buf, node_idx, buf_idx, i)
    l = NullableVector{Vector{UInt8},typeof(l)}(l, b)
    ConvertVector{Union{String,Missing},typeof(l)}(l), node_idx+1, buf_idx+2
end
#============================================================================================
    \end{from RecordBatch}
============================================================================================#

#============================================================================================
    \begin{build from schema field}
============================================================================================#
const CONTAINER_TYPES = (primitive=Union{Meta.Int_,Meta.FloatingPoint},
                         lists=Meta.List,
                         strings=Meta.Utf8,
                        )

# TODO incomplete
function _julia_eltype(ϕ::Meta.Field)
    if typeof(ϕ.dtype) <: CONTAINER_TYPES.primitive
        juliatype(ϕ.dtype)
    elseif typeof(ϕ.dtype) <: CONTAINER_TYPES.strings
        String
    elseif typeof(ϕ.dtype) <: CONTAINER_TYPES.lists
        Vector{julia_eltype(ϕ.children[1])}
    else
        throw(ArgumentError("unrecognized type $(ϕ.dtype)"))
    end
end
function _julia_eltype(ϕ::Meta.DictionaryEncoding)
    if typeof(ϕ.indexType) <: CONTAINER_TYPES.primitive
        juliatype(ϕ.indexType)
    else
        throw(ArgumentError("invalid dictionary index type $(ϕ.indexType)"))
    end
end
function _julia_eltype_nullable(ϕ::Union{Meta.Field,Meta.DictionaryEncoding})
    Union{_julia_eltype(ϕ),Missing}
end

"""
    julia_eltype(ϕ)

Gives the Julia element type of the Arrow `Field` metadata object.  For example, for an Arrow
`List<Int64>` this gives `Vector{Int64}` because the Julia object that is constructed to
represent this returns `Vector{Int64}` objects when indexed.
"""
julia_eltype(ϕ::Meta.Field) = ϕ.nullable ? _julia_eltype_nullable(ϕ) : _julia_eltype(ϕ)

"""
    julia_valtype(ϕ)

Gives the Julia values type of the Arrow `Field` metadata object.  The constructed object is
typically a subtype of this (though there is an exception because of how the arrow standard
decided to handle nullables).
"""
julia_valtype(ϕ::Meta.Field) = AbstractVector{julia_eltype(ϕ)}

"""
    julia_keytype(ϕ)

Gives the Julia type of the keys of the Arrow `Field` object if it represents a dictionary
encoding.  As far as I know, this is always a `AbstractVector{<:Union{Integer,Missing}}`.
"""
function julia_keytype(ϕ::Meta.Field)
    ϕ.dictionary == nothing && return nothing
    if ϕ.nullable
        AbstractVector{_julia_eltype_nullable(ϕ.dictionary)}
    else
        AbstractVector{_julia_eltype(ϕ.dictionary)}
    end
end

"""
    juliatype(ϕ)

Returns the Julia type corresponding to the Arrow `Field` metadata given by `ϕ`.

For dictionary fields, this returns the index type.
"""
Meta.juliatype(ϕ::Meta.Field) = julia_valtype(ϕ)

"""
    build

This function takes as its arguments Arrow metadata, which it then uses to call other methods
(with Julia metadata) for constructing arrays.
"""
function build(ϕ::Meta.Field, rb::Meta.RecordBatch, buf::Vector{UInt8}, node_idx::Integer=1,
               buf_idx::Integer=1, i::Integer=1)
    build(juliatype(ϕ), rb, buf, node_idx, buf_idx, i)
end
function build(ϕ::Meta.Field, rb::Meta.DictionaryBatch, buf::Vector{UInt8},
               node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    build(juliatype(ϕ), rb.data, buf, node_idx, buf_idx, i)
end
#============================================================================================
    \end{build from schema field}
============================================================================================#
