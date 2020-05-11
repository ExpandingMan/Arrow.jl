
# NOTE: a lot of this is slow right now, but 1.5 should drastically speed things up because
# of all the allocations from views

#======================================================================================================
    \begin{data serialization}
======================================================================================================#
function write!(buf::AbstractVector{UInt8}, ::Type{Primitive}, v::AbstractVector)
    v = reinterpret(UInt8, v)
    copyto!(buf, v)
    padding(length(v))
end
write!(io::IO, ::Type{Primitive}, v::AbstractVector) = writepadded(io, v)

function write!(buf::AbstractVector{UInt8}, ::Type{BitPrimitive}, v::AbstractVector)
    bitpack!(buf, v)
    bitpackedbytes(length(v))
end
function write!(io::IO, ::Type{BitPrimitive}, v::AbstractVector)
    bitpack!(io, v)
    bitpackedbytes(length(v))
end

function write!(buf::AbstractVector{UInt8}, ::typeof(bitmask), v::AbstractVector)
    bitpack!(buf, .!ismissing.(v))
    buf
end
write!(io::IO, ::typeof(bitmask), v::AbstractVector) = (bitpack!(io, .!ismissing.(v)); io)

function write!(buf::AbstractVector{UInt8}, ::typeof(offsets), v::AbstractVector)
    offsets!(reinterpret(DefaultOffset, buf), v)
    buf
end
function write!(io::IO, ::typeof(offsets), v::AbstractVector)
    last = zero(DefaultOffset)
    write(io, last)
    for j ∈ 2:(length(v)+1)
        next = DefaultOffset(offlength(v[j-1]) + last)
        write(io, next)
        last = next
    end
    io
end


# the below methods will determine the appropriate types

write!(buf::BufferOrIO, v::AbstractVector) = write!(buf, Primitive, v)

function write!(buf::BufferOrIO, v::AbstractVector{Union{T,Missing}}) where {T}
    write!(buf, bitmask, v)
    write!(buf, values, v)
end
#======================================================================================================
    \end{data serialization}
======================================================================================================#

#======================================================================================================
    \begin{arrow format for individual vectors}
======================================================================================================#
arrow(v::AbstractVector) = Primitive(v)
arrow(v::AbstractVector{Bool}) = BitPrimitive(v)
#======================================================================================================
    \end{arrow format for individual vectors}
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
