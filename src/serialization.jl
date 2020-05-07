
# NOTE: a lot of this is slow right now, but 1.5 should drastically speed things up because
# of all the allocations from views

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

# TODO continue from here

function serialize!(io::IO, sertype, v::AbstractVector, off::Integer=0)
    p = position(io)
    n = rawserialize!(io, sertype, v)
    Meta.Buffer(p+off, n)
end

function rawserialize!(buf::AbstractVector{UInt8}, i::Integer, ::Type{Primitive}, v::AbstractVector)
    writepadded!(buf, i, v)
end

# TODO again, the below methods are slow because they allocate buffers first
function rawserialize!(buf::AbstractVector{UInt8}, i::Integer, ::Type{BitPrimitive}, v::AbstractVector)
    writepadded!(buf, i, values(bitpack(v, pad=false)).buffer)
end
function rawserialize!(buf::AbstractVector{UInt8}, i::Integer, ::typeof(bitmask), v::AbstractVector)
    writepadded!(buf, i, values(bitmask(v, pad=false)).buffer)
end
function rawserialize!(buf::AbstractVector{UInt8}, i::Integer, ::typeof(offsets), v::AbstractVector)
    writepadded!(buf, i, offsets(v, pad=false).buffer)
end

function serialize!(buf::AbstractVector{UInt8}, i::Integer, sertype, v::AbstractVector, off::Integer=0)
    n = rawserialize!(buf, i, sertype, v)
    Meta.Buffer(i-1+off, n)
end


# TODO change these
function metadata(::Type{Meta.Field}, name::AbstractString, v::AbstractVector,
                  dtype::Meta.DType=arrowtype(eltype(v)))
    Meta.Field(name, false, dtype)
end
function metadata(::Type{Meta.Field}, name::AbstractString, v::AbstractVector{Union{T,Missing}},
                  dtype::Meta.DType=arrowtype(nonmissingtype(eltype(v)))) where {T}
    Meta.Field(name, true, dtype)
end

# TODO these will probably ultimately require a ton of work and are placeholder for now
metadata(::Type{Meta.FieldNode}, v::AbstractVector) = [Meta.FieldNode(length(v), count(ismissing, v))]

# TODO have to think long and hard what the API for all this shit is going to look like

function serialize!(io::IO, v::AbstractVector)
    metadata(Meta.FieldNode, v), [serialize!(io, Primitive, v)]
end


function serialize!(io::IO, name::AbstractString, v::AbstractVector)
    ns, bs = serialize!(io, v)
    [metadata(Meta.Field, name, v)], ns, bs
end
