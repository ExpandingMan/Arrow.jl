
# TODO we don't have serialization into buffers yet

#======================================================================================================
    \begin{data serialization}
======================================================================================================#
write!(io::IO, v::AbstractVector) = writepadded(io, v)
function writebits!(io::IO, v::AbstractVector{Bool})
    s = 0
    for i ∈ 1:8:length(v)
        j = min(i+7, lastindex(v))
        idx = i:j
        s += write(io, _bitpack_byte(view(v, idx), length(idx)))
    end
    δ = paddinglength(s)
    skip(io, δ)
    s + δ
end
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


function writemeta!(b::AbstractBatch{<:IO})
    io = b.buffer
    m = Meta.Message(b)
    s = write(io, CONTINUATION_INDICATOR_BYTES)
    mdata = FB.bytes(FB.build!(m))
    l = length(mdata)
    δ = paddinglength(l)
    s += write(io, Int32(l+δ))
    s += write(io, mdata)
    skip(io, δ)
    b.body_start = position(io) + 1  # body always starts immediatley after metadata
    s + δ
end

function writearray!(b::AbstractBatch{<:IO}, v::AbstractVector)
    io = b.buffer
    s = 0
    for (ctype, c) ∈ components(v)
        mb = getbuffer!(b)
        skip2position(io, bodystart(b)+mb.offset-1)
        s += if eltype(c) == Bool  # we only support writing bits for Bool vectors
            writebits!(io, c)
        else
            write!(io, c)
        end
    end
    s
end

function writedata!(b::AbstractBatch{<:IO}, vs)
    setnodeindex!(b)
    setbufferindex!(b)
    s = 0
    for v ∈ vs
        s += writearray!(b, v)
    end
    s
end
writedata!(b::EmptyBatch, vs) = 0
writedata!(b::EmptyBatch) = 0

write!(b::AbstractBatch, vs) = writemeta!(b) + writedata!(b, vs)
write!(b::EmptyBatch) = writemeta!(b)
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

function columns(names::AbstractVector{<:ColumnName}, vs, bs::AbstractVector{<:AbstractBatch})
    ϕs = [Meta.Field(n, vs) for (n, vs) ∈ zip(names, vs)]
    [Column(ϕ, bs) for ϕ ∈ ϕs]
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

function batchindices(n::Integer, l::Integer)
    a, r = divrem(l, n)
    if r ≠ 0
        throw(ArgumentError("array length must be integer divisible by number of batches, "*
                            "else specify custom batch indices"))
    end
    map(α -> (a*(α-1)+1):(a*α), 1:n)
end
batchindices(n::Integer, vs) = batchindices(n, _check_vector_lengths(vs))

# TODO this will have to change later to accommodate dictionary batches and stuff
# generates all batches except for the schema
function batches(locator::Function, io::IO, vs, o::Integer=position(io)+1;
                 nbatches::Integer=1, batch_indices=batchindices(nbatches, vs))
    bs = Vector{AbstractBatch}(undef, length(batch_indices))
    for (i, bi) ∈ enumerate(batch_indices)
        vsi = (view(v, bi) for v ∈ vs)
        bs[i] = RecordBatch(io, vsi, o)
        do_write && write!(bs[i], vsi)
    end
    bs
end
function batches(io::IO, vs, o::Integer=position(io)+1; kwargs...)
    batches(sequential_locator, io, vs, o; kwargs...)
end
function batches!(locator::Function, io::IO, vs, o::Integer=position(io)+1; kwargs...)
    bs = batches(loactor, io, vs, o; kwargs...)
    for b ∈ bs
        write!(b, vs)
    end
    bs
end
function batches!(io::IO, vs, o::Integer=position(io)+1; kwargs...)
    batches!(sequential_locator, io, vs, o; kwargs...)
end
#======================================================================================================
    \end{batch metadata}
======================================================================================================#

#======================================================================================================
    \begin{Table}
======================================================================================================#
function Table(locator::Function, io::IO, sch::Tables.Schema, vs, o::Integer=position(io)+1;
               nbatches::Integer=1, batch_indices=batchindices(nbatches, vs))
    h = EmptyBatch(Meta.Schema(sch), 0, io, o)
    bs = batches(locator, io, vs, o; nbatches=nbatches, batch_indices=batch_indices)
    cs = columns(sch.names, vs, bs)
    Table(h, cs)
end
function Table(io::IO, sch::Tables.Schema, vs, o::Integer=position(io)+1;
               nbatches::Integer=1, batch_indices=batchindices(nbatches, vs))
    Table(sequential_locator, io, sch, vs, o, nbatches=nbatches, batch_indices=batchindices)
end

# TODO table header probably needs to be an empty batch
function write!(t::Table, vs)
    write!(EmptyBatch(t.header)) + sum(write!(b, vs) for b ∈ batches(t))
end

function Table!(locator::Function, io::IO, sch::Tables.Schema, vs, o::Integer=position(io)+1;
                nbatches::Integer=1, batch_indices=batchindices(nbatches, vs))
    t = Table(locator, io, sch, vs, o; nbatches=nbatches, batch_indices=batch_indices)
    write!(t, vs)
    t
end
#======================================================================================================
    \end{Table}
======================================================================================================#
