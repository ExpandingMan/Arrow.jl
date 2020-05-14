#=====================================================================================================
    \begin{Values}

    TODO what about Bool and other "special" types?
=====================================================================================================#
struct Values{T,V<:AbstractVector} <: ArrowVector{T}
    parent::V
end

values_eltype(::Type{T}) where {T} = T
values_eltype(::Type{<:Types.List{T}}) where {T} = T
values_eltype(::Type{Types.Nullable{T}}) where {T} = Union{T,Unspecified}
values_eltype(::Type{Types.Nullable{T}}) where {T<:AbstractVector} = T

values_eltype(v::AbstractVector) = values_eltype(eltype(v))

Values(v::AbstractVector) = Values{values_eltype(v),typeof(v)}(v)

Base.parent(v::Values) = v.parent

value_length(::Type{Union{T,Unspecified}}, ::Missing) where {T} = 1
value_length(::Type, ::Missing) = 0
value_length(::Type, x) = length(x)

Base.size(v::Values) = (sum(value_length.((eltype(v),), v.parent)),)
Base.size(v::Values{<:Types.List}) = size(parent(v))

_values_return(::Type{T}, A, κ) where {T} = convert(T, A[κ])
function _values_return(::Type{Union{T,Unspecified}}, A, κ) where {T}
    ismissing(A) ? unspecified : convert(T, A[κ])
end

# adapted from LazyArrays.jl `vcat_getindex` method
function Base.getindex(v::Values, i::Integer)
    T = eltype(v)
    κ = i
    for A ∈ v.parent
        n = value_length(T, A)
        κ ≤ n && return _values_return(T, A, κ)
        κ -= n
    end
    throw(BoundsError(v, i))
end
function Base.getindex(v::Values{<:Types.List{T}}, i::Integer) where {T}
    ismissing(parent(v)[i]) ? T[] : parent(v)[i]
end

values(v::AbstractVector) = v
values(v::AbstractVector{Types.Nullable{T}}) where {T} = Values(v)
values(v::AbstractVector{<:AbstractVector}) = Values(v)
#====================================================================================================
    \end{Values}
====================================================================================================#

#============================================================================================
    \begin{Primitive Constructors}

    TODO get rid of most of this?
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
    \begin{BitVector Constructors}

    TODO do we need this?
============================================================================================#
function BitVector!(buf::Vector{UInt8}, v::AbstractVector{Bool}, i::Integer=1)
    write!(view(buf, i:lastindex(buf)), BitVector, v)
    BitVector(Primitive{UInt8}(buf, i, bitpackedbytes(length(v), false)), length(v))
end
function BitVector(v::AbstractVector{Bool})
    BitVector!(Vector{UInt8}(undef, bitpackedbytes(length(v))), v)
end
#============================================================================================
    \end{BitVector Constructors}
============================================================================================#

#============================================================================================
    \begin{bitmasks}

    TODO maybe have some nice methods here for writing bytes
============================================================================================#
struct BitMask{V<:AbstractVector} <: ArrowVector{Bool}
    values::V
end

Base.IndexLinear(::Type{<:BitMask}) = IndexLinear()
Base.size(b::BitMask) = size(b.values)

Base.getindex(b::BitMask, i::Integer) = !ismissing(b.values[i])

bitmask(v::AbstractVector) = BitMask(v)

bitmaskbytes(n::Integer; pad::Bool=true) = bitpackedbytes(n, pad)
bitmaskbytes(v::AbstractVector; pad::Bool=true) = bitmaskbytes(length(v), pad=pad)

# TODO probably want to get rid of this
function bitmask!(buf::Vector{UInt8}, v::AbstractVector, i::Integer=1)
    write!(view(buf, i:lastindex(buf)), bitmask, v)
    BitVector(Primitive{UInt8}(buf, i, bitpackedbytes(length(v), false)), length(v))
end
#============================================================================================
    \end{bitmasks}
============================================================================================#

#============================================================================================
    \begin{offsets}
============================================================================================#
"""
    offlength(v)

Compute the length of the object `v` for the purposes of determining offsets.  This defaults to
`length`, but has special handling for some cases like `AbstractString`.
"""
offlength(v) = length(v)
offlength(s::AbstractString) = ncodeunits(s)


struct Offsets{T,V<:AbstractVector} <: ArrowVector{T}
    values::V
end

Offsets(v::AbstractVector) = Offsets{DefaultOffset,typeof(v)}(v)

Base.IndexStyle(::Type{<:Offsets}) = IndexLinear()
Base.size(o::Offsets) = (length(o.values)+1,)

function Base.getindex(o::Offsets{T}, i::Integer) where {T}
    i == 1 ? zero(T) : convert(T, sum(offlength(o.values[j]) for j ∈ 1:(i-1)))
end

Base.iterate(o::Offsets{T}) where {T} = (zero(T), (2, zero(T)))
function Base.iterate(o::Offsets{T}, (i, last)) where {T}
    #println("boo!")  # it's not always abundantly obvious when this is being called
    i > length(o) && return nothing
    last = convert(T, last + offlength(o.values[i-1]))
    last, (i+1, last)
end


function offsetsbytes(n::Integer; pad::Bool=true)
    ℓ = (n+1)*sizeof(DefaultOffset)
    pad ? padding(ℓ) : ℓ
end
offsetsbytes(v::AbstractVector; pad::Bool=true) = offsetsbytes(length(v), pad=pad)

offsets(v::AbstractVector) = Offsets(v)

# NOTE: the below is of course slow if your offsets are lazy
offset(A::ArrowVector, i::Integer) = offsets(A)[i]
offset_range(A::ArrowVector, i::Integer) = offset(A, i):offset(A, i+1)
function offset1_range(A::ArrowVector, i::Integer)
    (offset(A, i)+1):offset(A, i+1)
end
#============================================================================================
    \end{offsets}
============================================================================================#
