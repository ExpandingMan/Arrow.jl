
const DefaultOffset = Int32

#============================================================================================
    \start{BitVector}

    NOTE: this shadows a name from `Base`, but it's rarely enough use that we'll just
    live with it.
============================================================================================#
struct BitVector{V<:AbstractVector{UInt8}} <: ArrowVector{Bool}
    values::V
    ℓ::Int
end

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

#============================================================================================
    \begin{List}
============================================================================================#
# note that T = eltype(eltype(l))
struct List{T,V<:AbstractVector{T}} <: ArrowVector{Vector{T}}
    values::V
    offsets::Primitive{DefaultOffset}
end

unmasked(l::List) = l

Base.size(l::List) = (length(offsets(l))-1,)
Base.getindex(l::List, i::Integer) = values(l)[offset1_range(l, i)]
#============================================================================================
    \end{List}
============================================================================================#

#============================================================================================
    \begin{NullableVector}

    NOTE: we don't constraint the values type `V` more because sometimes `T` is passed
    as `Vector{Union{S,Missing}}` and the eltype of `V` is `Vector{S}`.
============================================================================================#
struct NullableVector{T,M<:AbstractVector{Bool},V<:AbstractVector} <: ArrowVector{Union{T,Missing}}
    bitmask::M
    values::V
end

unmasked(v::NullableVector) = v.values
offsets(v::NullableVector) = unmasked(v).offsets

Base.size(v::NullableVector) = size(unmasked(v))
#============================================================================================
    \end{NullableVector}
============================================================================================#

#============================================================================================
    \begin{ConvertVector}
    Needed because `BroadcastArray` doesn't deal well with `String` stealing data
============================================================================================#
"""
    arrowconvert(T, x)

Convert arrow formatted data `x` to type `T`.  Typically this implies that a Julia object of
type `T` is represented in arrow by `x` and this function is called lazily to convert the
underlying data of an array.

Note that for strings, this calls `String` which steals the data in the provided
`Vector{UInt8}`.  This is appropriate for `List` objects since they return copies.
"""
arrowconvert(::Type, ::Missing) = missing
arrowconvert(::Type{<:Union{String,Missing}}, x::AbstractVector{UInt8}) = String(x)

struct ConvertVector{T,V<:AbstractVector} <: ArrowVector{T}
    values::V
end

Base.size(l::ConvertVector) = size(l.values)
Base.getindex(l::ConvertVector{T}, i::Integer) where {T} = arrowconvert(T, values(l)[i])
#============================================================================================
    \end{ConvertVector}
============================================================================================#

#============================================================================================
    \begin{Values}
============================================================================================#
struct Values{T,V<:AbstractVector} <: ArrowVector{T}
    parent::V
end

Values(v::AbstractVector{<:AbstractVector{T}}) where {T} = Values{T,typeof(v)}(v)
Values(v::AbstractVector) = Values{eltype(v),typeof(v)}(v)
function Values(v::AbstractVector{<:Union{<:AbstractVector{T},Missing}}) where {T}
    Values{T,typeof(v)}(v)
end

Base.parent(v::Values) = v.parent

_value_length(::Type, ::Missing) = 1
function _value_length(::Type{<:Values{T,<:AbstractVector{<:Union{<:AbstractVector,Missing}}}},
                       ::Missing) where {T}
    0
end
_value_length(::Type, x) = length(x)

Base.size(v::Values) = (sum(_value_length.((typeof(v),), v.parent)),)

# copied from LazyArrays.jl `vcat_getindex` method
function Base.getindex(v::Values, i::Integer)
    T = eltype(v)
    κ = i
    for A ∈ v.parent
        n = _value_length(typeof(v), A)
        κ ≤ n && return ismissing(A) ? A : convert(T,A[κ])::T
        κ -= n
    end
    throw(BoundsError(v, i))
end

values(v::Values) = v
values(v::AbstractVector) = Values(v)
#============================================================================================
    \end{Values}
============================================================================================#

#============================================================================================
    \begin{DictVector}
============================================================================================#
struct DictVector{T,K<:AbstractVector,V<:AbstractVector} <: ArrowVector{T}
    keys::K
    values::V
end

function DictVector(keys::AbstractVector{T}, vals::AbstractVector{U}) where {T,U}
    DictVector{U,typeof(keys),typeof(vals)}(keys, vals)
end
function DictVector(keys::AbstractVector{Union{T,Missing}}, vals::AbstractVector{U}
                   ) where {T,U}
    DictVector{Union{U,Missing},typeof(keys),typeof(vals)}(keys, vals)
end

Base.keytype(l::DictVector) = eltype(l.keys)
Base.valtype(l::DictVector) = eltype(l)

Base.size(l::DictVector) = size(l.keys)
function Base.getindex(l::DictVector{T,K}, i::Integer) where {T,K<:AbstractVector{<:Integer}}
    l.values[l.keys[i]+one(keytype(l))]
end
function Base.getindex(l::DictVector{T,K}, i::Integer
                      ) where {T,K<:AbstractVector{<:Union{Integer,Missing}}}
    k = l.keys[i]
    ismissing(k) && return missing
    l.values[k+one(keytype(l))]
end
#============================================================================================
    \end{DictVector}
============================================================================================#
