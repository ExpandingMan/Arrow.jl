
# this file should mirror Message.fbs

@STRUCT struct FieldNode
    length::Int64
    null_count::Int64
end
@ALIGN FieldNode 8

@enum(CompressionType::Int8, LZ4_FRAME=0, ZSTD=1)

@enum(BodyCompressionMethod::Int8, BUFFER=0)

@with_kw mutable struct BodyCompression
    codec::CompressionType = LZ4_FRAME
    method::BodyCompressionMethod = BUFFER
end
@ALIGN BodyCompression 1
FB.slot_offsets(::Type{BodyCompression}) = UInt32[4,6]

@with_kw mutable struct RecordBatch
    length::Int64 = 0
    nodes::Vector{FieldNode} = []
    buffers::Vector{Buffer} = []
    compression::Union{BodyCompression,Nothing} = nothing
end
@ALIGN RecordBatch 1
FB.slot_offsets(::Type{RecordBatch}) = UInt32[4,6,8,10]

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
    custom_metadata::Vector{KeyValue} = []
end
@ALIGN Message 1
FB.slot_offsets(::Type{Message}) = UInt32[4,6,8,10,12]
FB.root_type(::Type{Message}) = true
