
@with_kw mutable struct TensorDim
    size::Int64 = 0
    name::String = ""
end
@ALIGN TensorDim 1
FB.slot_offsets(::Type{TensorDim}) = UInt32[4,6]

@with_kw mutable struct Tensor
    dtype_type::UInt8 = 0
    dtype::DType = nothing
    shape::Vector{TensorDim} = []
    strides::Vector{Int64} = []
    data::Union{Buffer,Nothing} = nothing
end
@ALIGN Tensor 1
FB.slot_offsets(::Type{Tensor}) = UInt32[4,6,8,10,12]
FB.root_type(::Type{Tensor}) = true
