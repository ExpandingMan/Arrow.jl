
const DefaultOffset = Int32


#============================================================================================
    \begin{List}
============================================================================================#
# note that T = eltype(eltype(l))
struct List{V,T} <: ArrowVector{Vector{T}}
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
    \begin{NullableList}
============================================================================================#
struct NullableList{V,T} <: ArrowVector{Union{Vector{T},Missing}}
    list::List{V,T}
    bitmask::BitPrimitive
end

unmasked(l::NullableList) = l.list

Base.size(l::NullableList) = (length(unmasked(l)),)
function Base.setindex!(l::NullableList, v, i)
    throw(NotImplementedError("Cannot set indices on `NullableList` objects."))
end
#============================================================================================
    \end{NullableList}
============================================================================================#

