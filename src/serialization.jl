
# TODO we don't have serialization into buffers yet

#======================================================================================================
    \begin{data serialization}
======================================================================================================#
write!(io::IO, v::AbstractVector) = writepadded(io, v)
function write!(io::IO, v::AbstractVector{Union{T,Unspecified}}) where {T}
    s = 0
    for x ∈ v
        s += if x ≡ unspecified
            skip(io, sizeof(T)); sizeof(T)
        else
            write(io, x)
        end
    end
    skip(io, paddinglength(s))
    s + paddinglength(s)
end

function write!(rb::RecordBatch, v::AbstractVector)
    s = 0
    for (ctype, c) ∈ components(v)
        s += write!(rb, c)
    end
    s
end


function writemeta!(b::AbstractBatch{<:IO})
    io = b.buffer
    m = Meta.Message(b)
    s = write(io, 0xffffffff)
    # TODO wtf, bogus message being written
    mdata = FB.bytes(FB.build!(m))
    l = length(mdata)
    δ = paddinglength(l)
    s += write(io, Int32(l+δ))
    s += write(io, mdata)
    skip(io, δ)
    s + δ
end
#======================================================================================================
    \end{data serialization}
======================================================================================================#

#======================================================================================================
    \begin{column meta}
======================================================================================================#
function Meta.Field(name::ColumnName, v::AbstractVector; custom_metadata=Dict())
    Meta.Field(name, eltype(v); custom_metadata=custom_metadata)
end

newcolumn(ϕ::Meta.Field) = Column(ϕ, Vector{AbstractBatch}(undef, 0))
function newcolumn(name::ColumnName, v::AbstractVector; custom_metadata=Dict())
    newcolumn(Meta.Field(name, v, custom_metadata=custom_metadata))
end

function batch!(c::Column, b::AbstractBatch)
    init = length(batches(c)) == 0 ? 1 : nothing
    push!(c.node_start_idx, init)
    push!(c.buf_start_idx, init)
    push!(c.batches, b)
end
#======================================================================================================
    \end{column meta}
======================================================================================================#

#======================================================================================================
    \begin{batch metadata}
======================================================================================================#
function Meta.Schema(sch::Tables.Schema; custom_metadata=Dict())
    Meta.Schema(Meta.EndiannessLittle,
                # TODO need way to put more of the metadata in here eventually
                [Meta.Field(n, dtype) for (n, dtype) ∈ zip(sch.names, sch.types)],
                [Meta.KeyValue(kv) for kv ∈ custom_metadata])
end

# NOTE: the below takes ths specfic form so that it also works with NamedTuple
function _check_vector_lengths(vs,
                               msg::AbstractString="all vectors must have equal length")
    l = length(first(vs))
    for v ∈ vs
        if length(v) ≠ l
            throw(ArgumentError(msg))
        end
    end
    l
end

# TODO need to handle child nodes!
Meta.FieldNode(v::AbstractVector) = Meta.FieldNode(length(v), count(ismissing, v))
Meta.Buffer(v::AbstractVector, o::Integer, n::Integer=nbytes(v)) = Meta.Buffer(o-1, n)

function Meta.Message(rb::AbstractBatch; custom_metadata=Dict())
    Meta.Message(rb.header, bodylength(rb), custom_metadata=custom_metadata)
end

# using the below form so it works on NamedTuple
metadata(::Type{Meta.FieldNode}, vs) = collect(Meta.FieldNode(v) for v ∈ vs)
metadata!(ϕs::AbstractVector{Meta.FieldNode}, v::AbstractVector) = push!(ϕs, Meta.FieldNode(v))

function bodylength(mbufs::AbstractVector{Meta.Buffer}, o::Integer=1)
    isempty(mbufs) && return o
    lbuf = last(mbufs)
    lbuf.offset + padding(lbuf.length) + o
end
bodylength(rb::Meta.RecordBatch, o::Integer=1) = bodylength(rb.buffers, o)

"""
    sequential_locator(v, ctype, c, o, vidx, cidx)

This is the default function for computing the position of the sub-buffers of `v::AbstractVector` to be
written to an arrow record batch.  This simple locator creates a "dense" (up to padding) sequential
set of buffers by simply always returning the current offset `o`.

## Arguments
- `v::AbstractVector`: The current array to be serialized.
- `ctype`: The component type of the current array to be serialized (e.g. `bitmask`, `values`).
- `c::AbstractVector`: The current component to be serialized.
- `o::Integer`: The current offset (from the start of the message body) in the record batch body.
- `vidx::Integer`: The index of the current array `v` in the table or set of arrays.
- `cidx::Integer`: The index of the current components `c` of `v`.

## Returns (`Integer`)
The starting (1-based) offset of the buffer to be written.
"""
function sequential_locator(v::AbstractVector, ctype, c::AbstractVector, o::Integer, vidx::Integer,
                            cidx::Integer)
    o
end

function metadata!(locator::Function, mbufs::AbstractVector{Meta.Buffer}, v::AbstractVector,
                   vidx::Integer)
    for (cidx, (ctype, c)) ∈ enumerate(components(v))
        o = locator(v, ctype, c, bodylength(mbufs), vidx, cidx)
        push!(mbufs, Meta.Buffer(c, o))
    end
    mbufs
end
function metadata!(mbufs::AbstractVector{Meta.Buffer}, v::AbstractVector, vidx::Integer)
    metadata!(sequential_locator, mbufs, v, vidx)
end
function metadata(locator::Function, ::Type{Meta.Buffer}, vs)
    mbufs = Vector{Meta.Buffer}(undef, 0)
    for (vidx, v) ∈ enumerate(vs)
        metadata!(locator, mbufs, v, vidx)
    end
    mbufs
end
metadata(::Type{Meta.Buffer}, vs) = metadata(sequential_locator, Meta.Buffer, vs)

function Meta.RecordBatch(locator::Function, vs)
    l = _check_vector_lengths(vs)
    nodes = metadata(Meta.FieldNode, vs)
    mbufs = metadata(locator, Meta.Buffer, vs)
    Meta.RecordBatch(l, nodes, mbufs)
end

# TODO might need a way to delay passing the IO
function RecordBatch(locator::Function, io::IO, vs, o::Integer=position(io)+1)
    header = Meta.RecordBatch(locator, vs)
    RecordBatch(header, io, bodylength(header)-1, o)
end
RecordBatch(io::IO, vs, o::Integer=position(io)+1) = RecordBatch(sequential_locator, io, vs, o)
#======================================================================================================
    \end{batch metadata}
======================================================================================================#
