
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
    \begin{StringVector}
    Needed because `BroadcastArray` doesn't deal well with `String` stealing data
============================================================================================#
struct StringVector{T<:Union{Missing,String},
                    V<:AbstractVector{<:Union{Vector{UInt8},Missing}}} <: ArrowVector{T}
    values::V
end

Base.size(l::StringVector) = size(l.values)
Base.getindex(l::StringVector, i::Integer) = stringify(values(l)[i])
#============================================================================================
    \end{StringVector}
============================================================================================#
