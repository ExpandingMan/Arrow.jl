
# this file should mirror Message.fbs

@with_kw mutable struct FieldNode
    length::Int64
    null_count::Int64
end

@with_kw mutable struct RecordBatch
    length::Int64
    nodes::Vector{FieldNode}
    buffers::Vector{Buffer}
end

@with_kw mutable struct DictionaryBatch
    id::Int64
    data::RecordBatch
    # If isDelta is true the values in the dictionary are to be appended to a
    # dictionary with the indicated id
    isDelta::Bool = false
end

# TODO need to do tensors!!
@UNION MessageHeader (Schema,DictionaryBatch,RecordBatch)

@with_kw mutable struct Message
    version::MetadataVersion
    header::MessageHeader
    bodyLength::Int64
end
