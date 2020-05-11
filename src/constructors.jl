
#============================================================================================
    \begin{Primitive Constructors}
============================================================================================#
function Primitive!(buf::Vector{UInt8}, v::AbstractVector, i::Integer=1)
    write!(view(buf, i:lastindex(buf)), Primitive, v)
    Primitive{eltype(v)}(buf, i, length(v))
end
function Primitive(v::AbstractVector, i::Integer=1)
    Primitive!(Vector{UInt8}(undef, length(v)*sizeof(eltype(v))+i-1), v, i)
end

function valuesbytes(v::AbstractVector{T}; pad::Bool=true) where {T} 
    n = length(v)*sizeof(T)
    pad ? padding(n) : n
end
#============================================================================================
    \end{Primitive Constructors}
============================================================================================#

#============================================================================================
    \begin{BitPrimitive Constructors}
============================================================================================#
function BitPrimitive!(buf::Vector{UInt8}, v::AbstractVector{Bool}, i::Integer=1)
    write!(view(buf, i:lastindex(buf)), BitPrimitive, v)
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
    write!(view(buf, i:lastindex(buf)), bitmask, v)
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
    write!(view(buf, i:lastindex(buf)), offsets, v)
    Primitive{DefaultOffset}(buf, i, length(v)+1)
end
offsets(v::AbstractVector) = offsets!(Vector{UInt8}(undef, offsetsbytes(v)), v)
#============================================================================================
    \end{offsets}
============================================================================================#
