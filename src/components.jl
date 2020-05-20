#=====================================================================================================
    \begin{Values}

    TODO what about Bool and other "special" types?
=====================================================================================================#
struct Unmask{T,V<:AbstractVector} <: ArrowVector{T}
    parent::V
end

Unmask(v::AbstractVector{Types.Nullable{T}}) where {T<:Types.List} = Unmask{T,typeof(v)}(v)
Unmask(v::AbstractVector{Types.Nullable{T}}) where {T} = Unmask{Union{T,Unspecified},typeof(v)}(v)

Base.parent(v::Unmask) = v.parent

Base.size(v::Unmask) = size(parent(v))

function Base.getindex(v::Unmask{K}, i::Integer) where {T,K<:Types.List{T}}
    convert(K, ismissing(parent(v)[i]) ? T[] : parent(v)[i])
end
function Base.getindex(v::Unmask{Union{T,Unspecified}}, i::Integer) where {T}
    ismissing(parent(v)[i]) ? unspecified : convert(T, parent(v)[i])
end


struct Flatten{T,V<:AbstractVector} <: ArrowVector{T}
    parent::V
end

Flatten(v::AbstractVector{<:Types.List{T}}) where {T} = Flatten{T,typeof(v)}(v)
function Flatten(v::AbstractVector{<:Types.Strings}) 
    vv = codeunits.(v)
    Flatten{UInt8,typeof(vv)}(vv)
end

Base.parent(v::Flatten) = v.parent

Base.size(v::Flatten) = (sum(length.(parent(v))),)

function Base.getindex(v::Flatten, i::Integer)
    κ = i
    for A ∈ parent(v)
        n = length(A)
        κ ≤ n && return convert(eltype(v), A[κ])
        κ -= n
    end
    throw(BoundsError(v, i))
end

values(v::AbstractVector) = v
values(v::AbstractVector{Types.Nullable{T}}) where {T} = Unmask(v)
values(v::AbstractVector{<:Types.List}) = Flatten(v)
values(v::AbstractVector{<:Types.Strings}) = Flatten(v)
#====================================================================================================
    \end{Values}
====================================================================================================#

#============================================================================================
    \begin{bitmasks}

    TODO maybe have some nice methods here for writing bytes
============================================================================================#
struct BitMask{V<:AbstractVector} <: ArrowVector{Bool}
    values::V
end

Base.size(b::BitMask) = size(b.values)

Base.getindex(b::BitMask, i::Integer) = !ismissing(b.values[i])

bitmask(v::AbstractVector) = BitMask(v)
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

# NOTE: the below is pretty slow... will it be better in 1.5?
offset(A::ArrowVector, i::Integer) = offsets(A)[i]
offset_range(A::ArrowVector, i::Integer) = offset(A, i):offset(A, i+1)
function offset1_range(A::ArrowVector, i::Integer)
    (offset(A, i)+1):offset(A, i+1)
end
#============================================================================================
    \end{offsets}
============================================================================================#

#============================================================================================
    \begin{counting bytes}

    NOTE: these are only intended to be called on bits data as it's being written
============================================================================================#
nbytes(v::AbstractVector{Bool}) = bitpackedbytes(length(v))
nbytes(v::AbstractVector) = padding(length(v)*sizeof(eltype(v)))
#============================================================================================
    \end{counting bytes}
============================================================================================#
