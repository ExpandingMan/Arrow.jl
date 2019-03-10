module Arrow

using CategoricalArrays

using Base: @propagate_inbounds


const ALIGNMENT = 8


abstract type ArrowVector{T} <: AbstractVector{T} end


include("utils.jl")
include("primitives.jl")


end  # module Arrow
