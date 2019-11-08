
rawserialize!(io::IO, ::Type{Primitive}, v::AbstractVector) = writepadded(io, v)

# TODO the below methods are slow because they allocate buffers first
function rawserialize!(io::IO, ::Type{BitPrimitive}, v::AbstractVector)
    writepadded(io, values(bitpack(v, pad=false)).buffer)
end
function rawserialize!(io::IO, ::typeof(bitmask), v::AbstractVector)
    writepadded(io, values(bitmask(v, pad=false)).buffer)
end
function rawserialize!(io::IO, ::typeof(offsets), v::AbstractVector)
    writepadded(io, offsets(v, pad=false).buffer)
end

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
