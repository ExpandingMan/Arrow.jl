
# NOTE: a lot of this is slow right now, but 1.5 should drastically speed things up because
# of all the allocations from views

#======================================================================================================
    \begin{data serialization}
======================================================================================================#
function serialize!(buf::AbstractVector{UInt8}, ::Type{Primitive}, v::AbstractVector)
    copyto!(buf, reinterpret(UInt8, v))
    buf
end
serialize!(io::IO, ::Type{Primitive}, v::AbstractVector) = (writepadded(io, v); io)

function serialize!(buf::AbstractVector{UInt8}, ::Type{BitPrimitive}, v::AbstractVector)
    bitpack!(buf, v)
    buf
end
serialize!(io::IO, ::Type{BitPrimitive}, v::AbstractVector) = (bitpack!(io, v); io)

function serialize!(buf::AbstractVector{UInt8}, ::typeof(bitmask), v::AbstractVector)
    bitpack!(buf, .!ismissing.(v))
    buf
end
serialize!(io::IO, ::typeof(bitmask), v::AbstractVector) = (bitpack!(io, .!ismissing.(v)); io)

function serialize!(buf::AbstractVector{UInt8}, ::typeof(offsets), v::AbstractVector)
    offsets!(reinterpret(DefaultOffset, buf), v)
    buf
end
function serialize!(io::IO, ::typeof(offsets), v::AbstractVector)
    last = zero(DefaultOffset)
    write(io, last)
    for j ∈ 2:(length(v)+1)
        next = DefaultOffset(offlength(v[j-1]) + last)
        write(io, next)
        last = next
    end
    io
end
#======================================================================================================
    \end{data serialization}
======================================================================================================#


#======================================================================================================
    \begin{schemas}
======================================================================================================#
function Meta.Schema(sch::Tables.Schema; custom_metadata=Dict())
    Meta.Schema(Meta.EndiannessLittle,
                # TODO need way to put more of the metadata in here eventually
                [Meta.Field(n, dtype) for (n, dtype) ∈ zip(sch.names, sch.types)],
                [Meta.KeyValue(kv) for kv ∈ custom_metadata])
end
#======================================================================================================
    \end{schemas}
======================================================================================================#
