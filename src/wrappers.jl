
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
struct NullableVector{T,V<:AbstractVector} <: ArrowVector{Union{T,Missing}}
    values::V
    bitmask::BitPrimitive
end

unmasked(v::NullableVector) = v.values
offsets(v::NullableVector) = unmasked(v).offsets

Base.size(v::NullableVector) = size(unmasked(v))
#============================================================================================
    \end{NullableVector}
============================================================================================#
