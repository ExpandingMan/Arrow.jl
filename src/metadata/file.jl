
@STRUCT struct Block
    offset::Int64
    metadataLength::Int32
    bodyLength::Int64
end
@ALIGN Block 8

@with_kw mutable struct Footer
    version::MetadataVersion = 0
    schema::Union{Schema,Nothing} = nothing
    dictionaries::Vector{Block} = []
    recordBatches::Vector{Block} = []
    custom_metadata::Vector{KeyValue} = []
end
@ALIGN Footer 1
FB.slot_offsets(::Type{Footer}) = UInt32[4,6,8,10,12]
FB.root_type(::Type{Footer}) = true
