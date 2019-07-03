
const DefaultOffset = Int32


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
function Base.setindex!(l::List, v, i)
    throw(NotImplementedError("Cannot set indices on `List` objects."))
end
#============================================================================================
    \end{List}
============================================================================================#

#============================================================================================
    \begin{NullableVector}
============================================================================================#
struct NullableVector{T,V<:AbstractVector{<:Union{T,Missing}}} <: ArrowVector{Union{T,Missing}}
    values::V
    bitmask::BitPrimitive
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
