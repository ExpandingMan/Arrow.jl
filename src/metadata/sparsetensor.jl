
@with_kw mutable struct SparseTensorIndexCOO
    indicesBuffer::Union{Buffer,Nothing} = nothing
end
@ALIGN SparseTensorIndexCOO 1
FB.slot_offsets(::Type{SparseTensorIndexCOO}) = UInt32[4]

@with_kw mutable struct SparseMatrixIndexCSR
    indptrBuffer::Union{Buffer,Nothing} = nothing
    indicesBuffer::Union{Buffer,Nothing} = nothing
end
@ALIGN SparseMatrixIndexCSR 1
FB.slot_offsets(::Type{SparseMatrixIndexCSR}) = UInt32[4,6]

@UNION SparseTensorIndex (Nothing,SparseTensorIndexCOO,SparseMatrixIndexCSR)

@with_kw mutable struct SparseTensor
    dtype_type::UInt8 = 0
    dtype::DType = nothing
    shape::Vector{TensorDim} = []
    non_zero_length::Int64 = 0
    sparseIndex_type::UInt8 = 0
    sparseIndex::SparseTensorIndex = nothing
    data::Union{Buffer,Nothing} = nothing
end
@ALIGN SparseTensor 1
FB.slot_offsets(::Type{SparseTensor}) = UInt32[4,6,8,10,12,14,16]
FB.root_type(::Type{SparseTensor}) = true
