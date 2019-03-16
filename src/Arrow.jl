module Arrow

using CategoricalArrays

using Base: @propagate_inbounds


const ALIGNMENT = 8


struct NotImplementedError <: Exception
    msg::String
end


# TODO might change the name of this to "RawVector" or something
abstract type ArrowVector{T} <: AbstractVector{T} end


"""
    values(A)

Gets the Arrow `Primitive` array which contains the values of `A`.
"""
values(A::ArrowVector) = A.values

"""
    bitmask(A)

Gets the Arrow `BitPrimitive` array which contains the null bitmask of `A`.
"""
bitmask(A::ArrowVector) = A.bitmask

"""
    offsets(A)

Gets the Arrow `Primitive` array which contains the offsets of `A`.
"""
offsets(A::ArrowVector) = A.offsets

"""
    unmasked(A)

Gets the values prior to the application of the bitmask.
"""
unmasked(A::ArrowVector) = values(A)

offset(A::ArrowVector, i::Integer) = offsets(unmasked(A))[i]
offset_range(A::ArrowVector, i::Integer) = offset(unmasked(A), i):offset(unmasked(A), i+1)
offset1(A::ArrowVector, i::Integer) = offset(unmasked(A), i) + 1
offset1_range(A::ArrowVector, i::Integer) = offset1(unmasked(A), i):offset1(unmasked(A), i+1)

Base.size(p::ArrowVector) = size(unmasked(p))

function Base.getindex(p::ArrowVector{Union{T,Missing}}, i::Integer) where {T}
    bitmask(p)[i] ? @inbounds(unmasked(p)[i]) : missing
end

function Base.setindex!(p::ArrowVector{Union{T,Missing}}, ::Missing, i::Integer) where {T}
    bitmask(p)[i] = false
    missing
end
function Base.setindex!(p::ArrowVector{Union{T,Missing}}, v, i::Integer) where {T}
    bitmask(p)[i] = true  # bounds checking done here only
    @inbounds unmasked(p)[i] = convert(T, v)
    v
end

include("metadata/Metadata.jl")
using .Metadata; const Meta = Metadata

include("utils.jl")
include("primitives.jl")
include("lists.jl")
include("structs_unions.jl")
include("constructors.jl")


end  # module Arrow
