
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


abstract type AbstractUnionVector{T} <: ArrowVector{T} end

# TODO do union types
