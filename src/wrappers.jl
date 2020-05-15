#============================================================================================
    \begin{NullableVector}
============================================================================================#
struct NullableVector{T,M<:AbstractVector{Bool},V<:AbstractVector} <: ArrowVector{Union{T,Missing}}
    bitmask::M
    values::V
end

specifiedtype(::Type{T}) where {T} = T
specifiedtype(::Type{Union{T,Unspecified}}) where {T} = T

function NullableVector(b::AbstractVector{Bool}, v::AbstractVector)
    T = Union{specifiedtype(eltype(v)),Missing}
    NullableVector{Union{T,Missing},typeof(b),typeof(v)}(b, v)
end

bitmask(v::NullableVector) = v.bitmask
values(v::NullableVector) = v.values

Base.size(v::NullableVector) = size(values(v))
#============================================================================================
    \end{NullableVector}
============================================================================================#

#============================================================================================
    \begin{List}
============================================================================================#
# note that T = eltype(eltype(l))
struct List{T,O<:AbstractVector{<:Integer},V<:AbstractVector{T}} <: ArrowVector{Vector{T}}
    offsets::O
    values::V
end

List(o::AbstractVector, v::AbstractVector) = List{eltype(v),typeof(o),typeof(v)}(o, v)

offsets(l::List) = l.offsets
values(l::List) = l.values

Base.size(l::List) = (length(offsets(l))-1,)
Base.getindex(l::List, i::Integer) = values(l)[offset1_range(l, i)]
#============================================================================================
    \end{List}
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

# WARNING: have to be very careful using this because right now this steals strings
struct ConvertVector{T,V<:AbstractVector} <: ArrowVector{T}
    values::V
end

ConvertVector{T}(v::AbstractVector) where {T} = ConvertVector{T,typeof(v)}(v)

Base.size(l::ConvertVector) = size(l.values)
Base.getindex(l::ConvertVector{T}, i::Integer) where {T} = arrowconvert(T, l.values[i])
#============================================================================================
    \end{ConvertVector}
============================================================================================#

#============================================================================================
    \begin{StructVector}
============================================================================================#
struct StructVector{T<:Tuple,N,V<:NTuple{N,ArrowVector}} <: ArrowVector{T}
    values::V
end

# NOTE: we assume that all components have the same length
Base.size(s::StructVector) = size(first(values(s)))
function Base.getindex(s::StructVector{T,N}, i::Integer)::T where {T,N}
    ntuple(n -> values(s)[n][i], N)
end
#============================================================================================
    \end{StructVector}
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
