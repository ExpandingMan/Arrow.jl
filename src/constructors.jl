
# TODO need to do this also for IO!!!

#============================================================================================
    \begin{Primitive Constructors}
============================================================================================#
function Primitive!(buffer::Vector{UInt8}, v::AbstractVector, i::Integer=1)
    copyto!(buffer, i, reinterpret(UInt8, v))
    Primitive{eltype(v)}(buffer, i, length(v))
end
function Primitive(v::AbstractVector, blen::Integer=length(v)*sizeof(eltype(v)),
                   i::Integer=1)
    Primitive!(Vector{UInt8}(undef, blen), v, i)
end

function Base.write(io::IO, ::Type{Primitive}, v::AbstractVector)
    writepadded(io, reinterpret(UInt8, v))
end
#============================================================================================
    \end{Primitive Constructors}
============================================================================================#

#============================================================================================
    \begin{BitPrimitive Constructors}
============================================================================================#
function BitPrimitive!(buffer::Vector{UInt8}, v::AbstractVector{Bool}, i::Integer=1,
                       (ℓ,a,b)::Tuple=_bitpackedbytes(length(v), pad); pad::Bool=false)
    bitpack!(buffer, v, i, (a, b))
    BitPrimitive(Primitive{UInt8}(buffer, i, ℓ), length(v))
end
function BitPrimitive(v::AbstractVector{Bool}, blen::Integer, i::Integer=1; pad::Bool=true)
    BitPrimitive!(zeros(UInt8, blen), v, i, pad=pad)
end
function BitPrimitive(v::AbstractVector{Bool}; pad::Bool=true)
    ℓ, a, b = _bitpackedbytes(length(v), pad)
    BitPrimitive!(zeros(UInt8, ℓ), v, 1, (ℓ, a, b))
end

function Base.write(io::IO, ::Type{BitPrimitive}, v::AbstractVector)
    writepadded(io, values(bitpack(v, pad=false)).buffer)
end
#============================================================================================
    \end{BitPrimitive Constructors}
============================================================================================#

#============================================================================================
    \begin{bitmasks}
============================================================================================#
bitmaskbytes(n::Integer; pad::Bool=true) = bitpackedbytes(n, pad)
bitmaskbytes(v::AbstractVector; pad::Bool=true) = bitmaskbytes(length(v), pad=pad)

function bitmask!(buffer::Vector{UInt8}, v::AbstractVector, i::Integer=1; pad::Bool=true)
    BitPrimitive!(buffer, .!ismissing.(v), i)
end
function bitmask(v::AbstractVector, blen::Integer, i::Integer=1; pad::Bool=true)
    BitPrimitive(.!ismissing.(v), blen, i, pad=pad)
end
bitmask(v::AbstractVector; pad::Bool=true) = BitPrimitive(.!ismissing.(v), pad=pad)

function Base.write(io::IO, ::typeof(bitmask), v::AbstractVector)
    writepadded(io, values(bitmask(v, pad=false)).buffer)
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

# NOTE: the eltype constraint ensures that the size computation is correct
function offsets!(off::AbstractVector{DefaultOffset}, v::AbstractVector{T},
                  i::Integer=1) where {T<:Union{Vector,String}}
    off[i] = 0
    for j ∈ 2:(length(v)+1)
        off[i+j-1] = sizeof(v[j-1]) + off[j-1]
    end
    off
end
function offsets!(buffer::Vector{UInt8}, v::AbstractVector{T},
                  i::Integer=1) where {T<:Union{Vector,String}}
    offsets!(reinterpret(DefaultOffset, view(buffer, i:length(buffer))), v, i)
    Primitive{DefaultOffset}(buffer, i, length(v)+1)
end
function offsets(v::AbstractVector{T}, blen::Integer, i::Integer=1;
                 pad::Bool=true) where {T<:Union{Vector,String}}
    offsets!(zeros(UInt8, blen), v, i)
end
function offsets(v::AbstractVector{T}; pad::Bool=true) where {T<:Union{Vector,String}}
    offsets(v, offsetsbytes(v, pad=pad), i)
end

function Base.write(io::IO, ::typeof(offsets), v::AbstractVector{T}
                   ) where {T<:Union{Vector,String}}
    writepadded(io, offsets(v, pad=false).buffer)
end
#============================================================================================
    \end{offsets}
============================================================================================#

#============================================================================================
    \begin{from RecordBatch}
============================================================================================#
function primitive(::Type{T}, ϕn::Meta.FieldNode, b::Meta.Buffer, buf::Vector{UInt8},
                   i::Integer=1) where {T}
    Primitive{T}(buf, i + b.offset, fld(b.length, sizeof(T)))
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

# TODO is the ordering of the sub-buffers canonical???
# TODO less the below is the right way to do this
function build(::Type{Primitive{T}}, rb::Meta.RecordBatch, buf::Vector{UInt8},
               node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1) where {T}
    primitive(T, rb, buf, node_idx, buf_idx, i), node_idx+1, buf_idx+1
end
function build(::Type{NullablePrimitive{T}}, rb::Meta.RecordBatch, buf::Vector{UInt8},
               node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1) where {T}
    b = bitmask(rb, buf, node_idx, buf_idx, i)
    buf_idx += 1
    v = primitive(T, rb, buf, node_idx, buf_idx, i)
    NullablePrimitive{T}(v, b), node_idx+1, buf_idx+1
end
#============================================================================================
    \end{from RecordBatch}
============================================================================================#

#============================================================================================
    \begin{build from schema field}
============================================================================================#
const CONTAINER_TYPES = (primitive=Union{Meta.Int_,Meta.FloatingPoint},)

# TODO incomplete
function _juliatype(ϕ::Meta.Field)
    if typeof(ϕ.dtype) <: CONTAINER_TYPES.primitive
        Primitive{juliatype(ϕ.dtype)}
    else
        throw(ArgumentError("unrecognized type $(ϕ.dtype)"))
    end
end
function _juliatype_nullable(ϕ::Meta.Field)
    if typeof(ϕ.dtype) <: CONTAINER_TYPES.primitive
        NullablePrimitive{juliatype(ϕ.dtype)}
    else
        throw(ArgumentError("unrecognized type $(ϕ.dtype)"))
    end
end

Meta.juliatype(ϕ::Meta.Field) = ϕ.nullable ? _juliatype_nullable(ϕ) : _juliatype(ϕ)

function build(ϕ::Meta.Field, rb::Meta.RecordBatch, buf::Vector{UInt8}, node_idx::Integer=1,
               buf_idx::Integer=1, i::Integer=1)
    build(juliatype(ϕ), rb, buf, node_idx, buf_idx, i)
end
#============================================================================================
    \end{build from schema field}
============================================================================================#
