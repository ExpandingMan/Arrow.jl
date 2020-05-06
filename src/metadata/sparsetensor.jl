
@with_kw mutable struct SparseTensorIndexCOO
    indicesType::Int_
    indicesStrides::Vector{Int64} = []
    indicesBuffer::Buffer
end
@ALIGN SparseTensorIndexCOO 1
FB.slot_offsets(::Type{SparseTensorIndexCOO}) = UInt32[4,6,8]

@enum(SparseMatrixCompressedAxis::Int16, SMCARow=0, SMCAColumn=1)

@with_kw mutable struct SparseMatrixIndexCSX
    compressedAxis::SparseMatrixCompressedAxis = 0
    indptrType::Int_
    indptrBuffer::Buffer
    indicesType::Int_
    indicesBuffer::Buffer
end
@ALIGN SparseMatrixIndexCSX 1
FB.slot_offsets(::Type{SparseMatrixIndexCSX}) = UInt32[4,6,8,10,12]

@with_kw mutable struct SparseTensorIndexCSF
    indptrType::Int_
    indptrBuffers::Vector{Buffer}
    indicesType::Int_
    indicesBuffers::Vector{Buffer}
    axisOrder::Vector{Int32}
end
@ALIGN SparseTensorIndexCSF 1
FB.slot_offsets(::Type{SparseTensorIndexCSF}) = UInt32[4,6,8,10,12]

@UNION SparseTensorIndex (Nothing,
                          SparseTensorIndexCOO,
                          SparseMatrixIndexCSX,
                          SparseTensorIndexCSF,
                         )

@with_kw mutable struct SparseTensor
    dtype_type::UInt8 = 0
    dtype::DType
    shape::Vector{TensorDim}
    non_zero_length::Int64 = 0
    sparseIndex_type::UInt8 = 0
    sparseIndex::SparseTensorIndex
    data::Buffer
end
@ALIGN SparseTensor 1
FB.slot_offsets(::Type{SparseTensor}) = UInt32[4,6,8,10,12,14,16]
FB.root_type(::Type{SparseTensor}) = true
