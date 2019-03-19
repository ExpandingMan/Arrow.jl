
# this file should mirror Message.fbs

@STRUCT struct FieldNode
    length::Int64
    null_count::Int64
end
@ALIGN FieldNode 8

@with_kw mutable struct RecordBatch
    length::Int64 = 0
    nodes::Vector{FieldNode} = []
    buffers::Vector{Buffer} = []
end
@ALIGN RecordBatch 1
FB.slot_offsets(::Type{RecordBatch}) = UInt32[4,6,8]

@with_kw mutable struct DictionaryBatch
    id::Int64 = 0
    data::Union{RecordBatch,Nothing} = nothing
    # If isDelta is true the values in the dictionary are to be appended to a
    # dictionary with the indicated id
    isDelta::Bool = false
end
@ALIGN DictionaryBatch 1
FB.slot_offsets(::Type{DictionaryBatch}) = UInt32[4,6,8]

@UNION MessageHeader (Nothing,Schema,DictionaryBatch,RecordBatch,Tensor,SparseTensor)

@with_kw mutable struct Message
    version::MetadataVersion = 0
    header_type::UInt8 = 0
    header::MessageHeader = nothing
    bodyLength::Int64 = 0
end
@ALIGN Message 1
FB.slot_offsets(::Type{Message}) = UInt32[4,6,8,10]
FB.root_type(::Type{Message}) = true
