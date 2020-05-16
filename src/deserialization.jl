
# TODO we don't have deserialization from IO streams yet

#======================================================================================================
    \begin{general batches}
======================================================================================================#
abstract type AbstractBatch{B<:BufferOrIO} end

bodystart(b::AbstractBatch) = b.body_start
bodylength(b::AbstractBatch) = b.body_length
bodyend(b::AbstractBatch) = bodystart(b) + bodylength(b) - 1

reset!(b::AbstractBatch) = b

struct EmptyBatch{H,B<:BufferOrIO} <: AbstractBatch{B}
    header::H

    # this is kept only to keep track of location in the buffer
    buffer::B
    body_start::Int
    body_length::Int
end

batch(m, blen::Integer, buf::Vector{UInt8}, i::Integer) = EmptyBatch(m, buf, i, blen)

batch(m::Meta.Message, buf::Vector{UInt8}, i::Integer) = batch(m.header, m.bodyLength, buf, i)
#======================================================================================================
    \end{general batches}
======================================================================================================#

#======================================================================================================
    \begin{RecordBatch}
======================================================================================================#
mutable struct RecordBatch{B<:BufferOrIO} <: AbstractBatch{B}
    header::Meta.RecordBatch

    buffer::B
    body_start::Int
    body_length::Int

    node_idx::Int
    buf_idx::Int
end

function RecordBatch(m::Meta.RecordBatch, blen::Integer, buf::Vector{UInt8}, i::Integer)
    RecordBatch(m, buf, i, blen, 1, 1)
end
function RecordBatch(m::Meta.RecordBatch, io::IO, blen::Integer=0, i::Integer=position(io)+1)
    RecordBatch(m, io, i, blen, 1, 1)
end

setnodeindex!(rb::RecordBatch, idx::Integer=1) = (rb.node_idx = idx)
setbufferindex!(rb::RecordBatch, idx::Integer=1) = (rb.buf_idx = idx)

getnodeindex(rb::RecordBatch) = rb.node_idx
getbufferindex(rb::RecordBatch) = rb.buf_idx

reset!(rb::RecordBatch) = (setnodeindex!(rb); setbufferindex!(rb); rb)

batch(m::Meta.RecordBatch, blen::Integer, buf::Vector{UInt8}, i::Integer) = RecordBatch(m, blen, buf, i)

nnodes(rb::RecordBatch) = length(rb.header.nodes)
nbuffers(rb::RecordBatch) = length(rb.header.buffers)

getnode(rb::RecordBatch, idx=rb.node_idx) = rb.header.nodes[idx]
getbuffer(rb::RecordBatch, idx=rb.buf_idx) = rb.header.buffers[idx]

# these indices cycle
function getnode!(rb::RecordBatch)
    n = getnode(rb)
    rb.node_idx = mod1(rb.node_idx+1, nnodes(rb))
    n
end
function getbuffer!(rb::RecordBatch)
    b = getbuffer(rb)
    rb.buf_idx = mod1(rb.buf_idx+1, nbuffers(rb))
    b
end

function _check_empty_buffer(rb::RecordBatch, node_idx=rb.node_idx, buf_idx=rb.buf_idx)
    getnode(rb, node_idx).null_count == 0 && getbuffer(rb, buf_idx).length == 0
end

function buildnext(::Type{T}, rb::RecordBatch) where {T}
    Primitive{T}(rb.buffer, bodystart(rb)+getbuffer(rb).offset, getbuffer(rb).length ÷ sizeof(T))
end

function buildnext!(::Type{T}, rb::RecordBatch, ℓ::Integer) where {T}
    Primitive{T}(rb.buffer, bodystart(rb)+getbuffer!(rb).offset, ℓ)
end
function buildnext!(::Type{T}, rb::RecordBatch) where {T}
    getnode!(rb)
    b = getbuffer!(rb)
    Primitive{T}(rb.buffer, bodystart(rb)+b.offset, b.length ÷ sizeof(T))
end

buildnext!(::Type{Missing}, rb::RecordBatch) = Fill(missing, getnode!(rb).length)

function buildnext!(::typeof(offsets), rb::RecordBatch)
    n = getnode(rb)
    b = getbuffer!(rb)
    Primitive{DefaultOffset}(rb.buffer, bodystart(rb)+b.offset, b.length ÷ sizeof(DefaultOffset))
end

# NOTE: first node gets skipped only for strings for some unholy reason
function buildnext!(::Type{<:Types.List{T}}, rb::RecordBatch, skipnode::Bool=true) where {T}
    o = buildnext!(offsets, rb)
    skipnode && getnode!(rb)
    v = buildnext!(T, rb)
    List(o, v)
end

function buildnext!(::Type{<:Types.Strings}, rb::RecordBatch)
    l = buildnext!(Vector{UInt8}, rb, false)
    ConvertVector{String}(l)
end

# the below does not increment the node
function buildnext!(::typeof(bitmask), rb::RecordBatch)
    # need to check for empty buffers because they may be allowed
    if _check_empty_buffer(rb)
        getbuffer!(rb)
        return nothing
    end
    n = getnode(rb)
    b = getbuffer!(rb)
    BitVector(Primitive{UInt8}(rb.buffer, bodystart(rb)+b.offset, b.length), n.length)
end

function buildnext!(::Type{Union{T,Missing}}, rb::RecordBatch) where {T}
    b = buildnext!(bitmask, rb)
    v = buildnext!(T, rb)
    isnothing(b) ? v : NullableVector{T,typeof(b),typeof(v)}(b, v)
end

function build(ϕ::Meta.Field, rb::RecordBatch)
    T = isnothing(ϕ.dictionary) ? juliatype(ϕ) : julia_keytype(ϕ)
    build(T, ϕ, rb)
end

build(::Type{AbstractVector{T}}, ϕ::Meta.Field, rb::RecordBatch) where{T} = buildnext!(T, rb)

function build(ϕ::Meta.Field, rb::Meta.RecordBatch, buf::Vector{UInt8}, node_idx::Integer=1,
               buf_idx::Integer=1, i::Integer=1)
    build(juliatype(ϕ), rb, buf, node_idx, buf_idx, i)
end
#======================================================================================================
    \end{RecordBatch}
======================================================================================================#

#======================================================================================================
    \begin{DictionaryBatch}
======================================================================================================#
mutable struct DictionaryBatch{B<:BufferOrIO} <: AbstractBatch{B}
    header::Meta.DictionaryBatch
    record_batch::RecordBatch

    buffer::B
    body_start::Int
    body_length::Int
end

function DictionaryBatch(m::Meta.DictionaryBatch, blen::Integer, buf::Vector{UInt8}, i::Integer)
    # TODO is it ok to pass RecordBatch constructed with same body_start?
    DictionaryBatch(m, RecordBatch(m.data, blen, buf, i), buf, i, blen)
end
function batch(m::Meta.DictionaryBatch, blen::Integer, buf::Vector{UInt8}, i::Integer)
    DictionaryBatch(m, blen, buf, i)
end

dictionaryid(b::Meta.DictionaryBatch) = b.id
dictionaryid(ϕ::Meta.DictionaryEncoding) = ϕ.id
dictionaryid(ϕ::Meta.Field) = ϕ.dictionary == nothing ? nothing : dictionaryid(ϕ.dictionary)
dictionaryid(b::DictionaryBatch) = dictionaryid(b.header)

getnodeindex(db::DictionaryBatch) = getnodeindex(db.record_batch)
getbufferindex(db::DictionaryBatch) = getbufferindex(db.record_batch)

reset!(db::DictionaryBatch) = reset!(db.record_batch)

setnodeindex!(db::DictionaryBatch, idx::Integer=1) = setnodeindex!(db.record_batch, idx)
setbufferindex!(db::DictionaryBatch, idx::Integer=1) = setbufferindex!(db.record_batch, idx)

nnodes(db::DictionaryBatch) = nnodes(db.record_batch)
nbuffers(db::DictionaryBatch) = nbuffers(db.record_batch)

getnode(db::DictionaryBatch, idx=db.record_batch.node_idx) = getnode(db.record_batch, idx)
getbuffer(db::DictionaryBatch, idx=db.record_batch.buf_idx) = getbuffer(db.record_batch, idx)

getnode!(db::DictionaryBatch) = getnode!(db.record_batch)
getbuffer!(db::DictionaryBatch) = getbuffer!(db.record_batch)

build(ϕ::Meta.Field, db::DictionaryBatch) = build(juliatype(ϕ), ϕ, db.record_batch)

function build(ϕ::Meta.Field, rb::Meta.DictionaryBatch, buf::Vector{UInt8},
               node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    build(juliatype(ϕ), rb.data, buf, node_idx, buf_idx, i)
end
#======================================================================================================
    \end{DictionaryBatch}
======================================================================================================#

#======================================================================================================
    \begin{reading into memory}
======================================================================================================#
"""
    readmessage_length(io)

Arrow messages are prepended with the message header length as an `Int32`.  This function returns a
tuple `l, m` where `l` is that length and `m` is the message after reading them from the `IO`.

If the end of the file is reached `(0, nothing)` will be returned.
"""
function readmessage_length(io::IO)
    eof(io) && return (0, nothing)
    c = read(io, Int32)
    c ≠ -1 && return (0, nothing)  # we've gotten to something that isn't a message
    l = read(io, Int32)
    eof(io) && return (0, nothing)  # should we give warning for this
    m = readmessage(io, l)
    l, m
end

function batch(buf::Vector{UInt8}, rf::Integer=1, i::Integer=-1, databuf::Vector{UInt8}=buf)
    rf + 3 > length(buf) && return nothing
    c = reinterpret(Int32, buf[rf:(rf+3)])[1]
    c ≠ -1 && return nothing  # we've gotten to something that isn't a batch
    l = reinterpret(Int32, buf[(rf+4):(rf+7)])[1]
    l == 0 && return nothing
    m = readmessage(buf, rf+8)
    m == nothing && return nothing
    i < 1 && (i = rf+8+l)
    @bp
    batch(m, databuf, i)
end

function batch(io::IO, databuf::Vector{UInt8}, i::Integer=1)
    l, m = readmessage_length(io)
    l == 0 && return nothing
    batch(m, databuf, i)
end
function batch(io::IO, dataio::IO=io, data_skip::Integer=0)
    l, m = readmessage_length(io)
    l == 0 && return nothing
    skip(dataio, data_skip)
    databuf = read(dataio, m.bodyLength)
    batch(m, databuf, 1)
end

"""
    readbatches

Read all batches from a buffer or IO stream.  The reading will be attempted sequentially and
will terminate when the end of the stream or buffer or a `0` length specifier is encountered.
"""
function readbatches(buf::Vector{UInt8}, rf::AbstractVector{<:Integer},
                     i::AbstractVector{<:Integer}=fill(-1, length(rf)),
                     databuf::Vector{UInt8}=buf)
    batches = Vector{AbstractBatch}(undef, length(rf))
    for j ∈ 1:length(rf)
        batches[j] = batch(buf, rf[j], i[j], databuf)
    end
    batches
end
function readbatches(buf::Vector{UInt8}, rf::Integer=1, max_batches::Integer=typemax(Int))
    batches = Vector{AbstractBatch}(undef, 0)
    while length(batches) < max_batches
        b = batch(buf, rf)
        b == nothing && break
        push!(batches, b)
        rf = bodyend(b) + 1
    end
    batches
end
function readbatches(io::IO, dataio::IO=io, data_skip::Integer=0,
                     max_batches::Integer=typemax(Int))
    batches = Vector{AbstractBatch}(undef, 0)
    while length(batches) < max_batches
        b = batch(io, dataio, data_skip)
        b == nothing && break
        push!(batches, b)
    end
    batches
end
#======================================================================================================
    \end{reading into memory}
======================================================================================================#

#======================================================================================================
    \begin{Column}
======================================================================================================#
mutable struct Column
    header::Meta.Field
    batches::Vector{AbstractBatch}
    node_start_idx::Vector{Union{Int,Nothing}}  # start position of nodes
    buf_start_idx::Vector{Union{Int,Nothing}}  # start position of buffers
end

Meta.juliatype(c::Column) = Meta.juliatype(c.header)

reset!(c::Column) = (foreach(reset!, c.batches); c)

name(c::Column) = Symbol(c.header.name)
isnullable(c::Column) = c.header.nullable

batches(c::Column) = c.batches

nbatches(c::Column) = length(batches(c))

dictionaryid(c::Column) = dictionaryid(c.header)

getnodeindices(c::Column) = [getnodeindex(c.batches[i]) for i ∈ 1:nbatches(c)]
getbufferindices(c::Column) = [getbufferindex(c.batches[i]) for i ∈ 1:nbatches(c)]

function finddictionarybatch(c::Column)
    idx = findfirst(batches(c)) do b
        b isa DictionaryBatch && dictionaryid(b) == dictionaryid(c)
    end
    isnothing(idx) &&
        throw(ErrorException("can't find dictionary batch for column `$(name(c))`"))
    idx
end

"""
    setfirstcolumn!(c::Column)

Sets the indices appropriate for the first column in a schema.
"""
function setfirstcolumn!(c::Column)
    c.node_start_idx = fill(1, length(c.batches))
    c.buf_start_idx = fill(1, length(c.batches))
    c
end

function Column(ϕ::Meta.Field, bs::AbstractVector{<:AbstractBatch})
    Column(ϕ, bs, fill(nothing, length(bs)), fill(nothing, length(bs)))
end

function Column(sch::Meta.Schema, idx::Integer, batches::AbstractVector{<:AbstractBatch})
    Column(sch.fields[idx], batches)
end

function Column(ϕ::Meta.Field, buf::Vector{UInt8}, rf::AbstractVector{<:Integer},
                i::AbstractVector{<:Integer}=fill(-1, length(rf)), databuf::Vector{UInt8}=buf)
    Column(ϕ, readbatches(buf, rf, i, databuf))
end
function Column(ϕ::Meta.Field, buf::Vector{UInt8}, rf::Integer=1, max_batches::Integer=typemax(Int))
    Column(ϕ, readbatches(buf, rf, max_batches))
end
function Column(ϕ::Meta.Field, io::IO, dataio::IO=io, data_skip::Integer=0,
                max_batches::Integer=typemax(Int))
    Column(ϕ, readbatches(io, dataio, data_skip, max_batches))
end

function build(::Type{T}, c::Column) where {T<:AbstractBatch}
    idx = findall(b -> b isa T, batches(c))
    ntuple(i -> build(c, idx[i]), length(idx))
end
build(::Type{DictionaryBatch}, c::Column) = (build(c, finddictionarybatch(c)),)

julia_eltype(c::Column) = julia_eltype(c.header)

function build(c::Column)
    v = if !isnothing(c.header.dictionary)
        v = build(DictionaryBatch, c)
        k = build(RecordBatch, c)
        ntuple(i -> DictVector(k[i], v[1]), length(k))
    else
        build(RecordBatch, c)
    end
    # NOTE: creation of Vcat is pretty damn slow, so this may be a problem
    length(v) == 1 ? v[1] : Vcat(v...)
end

function build(c::Column, i::Integer)
    n, b = c.node_start_idx[i], c.buf_start_idx[i]
    if isnothing(n) || isnothing(b)
        throw(ErrorException("tried to build uninitialized column `$(name(c))`"))
    end
    setnodeindex!(c.batches[i], c.node_start_idx[i])
    setbufferindex!(c.batches[i], c.buf_start_idx[i])
    build(c.header, c.batches[i])
end
#======================================================================================================
    \end{Column}
======================================================================================================#

#======================================================================================================
    \begin{Table}
======================================================================================================#
struct Table
    header::Meta.Schema
    columns::Vector{Column}
end

reset!(t::Table) = (foreach(reset!, columns(t)); t)

columns(t::Table) = t.columns

column(t::Table, col::Integer) = columns(t)[col]
column(t::Table, col::Symbol) = columns(t)[findfirst(c -> name(c) == col, columns(t))]

ncolumns(sch::Meta.Schema) = length(sch.fields)
ncolumns(t::Table) = ncolumns(t.header)

function Table(batches::AbstractVector{<:AbstractBatch})
    if isempty(batches)
        throw(ArgumentError("no batches provided, undable to build Table"))
    end
    sch = first(batches).header
    cols = [Column(sch, i, batches[2:end]) for i ∈ 1:ncolumns(sch)]
    isempty(cols) || setfirstcolumn!(first(cols))
    Table(sch, cols)
end
function Table(buf::Vector{UInt8}, rf::AbstractVector{<:Integer},
               i::AbstractVector{<:Integer}=fill(-1, length(rf)),
               databuf::Vector{UInt8}=buf)
    Table(readbatches(buf, rf, i, databuf))
end
function Table(buf::Vector{UInt8}, rf::Integer=1, max_batches::Integer=typemax(Int))
    Table(readbatches(buf, rf, max_batches))
end
function Table(io::IO, dataio::IO=io, data_skip::Integer=0, max_batches::Integer=typemax(Int))
    Table(readbatches(io, dataio, data_skip, max_batches))
end

function build(t::Table, i::Integer)
    p = build(t.columns[i])
    if i < ncolumns(t)
        t.columns[i+1].node_start_idx = getnodeindices(t.columns[i])
        t.columns[i+1].buf_start_idx = getbufferindices(t.columns[i])
    end
    p
end
build(t::Table, s::Symbol) = build(t, findfirst(c -> name(c) == s, columns(t)))

build(t::Table) = (;(name(column(t, i))=>build(t, i) for i ∈ 1:ncolumns(t))...)

Tables.istable(::Table) = true
Tables.columnaccess(::Table) = true
Tables.columns(t::Table) = build(t)

Tables.schema(t::Table) = Tables.Schema(name.(t.columns), julia_eltype.(t.columns))
#======================================================================================================
    \end{Table}
======================================================================================================#

#======================================================================================================
    \begin{inferring schema}
======================================================================================================#
const CONTAINER_TYPES = (primitive=Set((Meta.Int_,Meta.FloatingPoint,Meta.Null)),
                         lists=Set((Meta.List,)),
                         strings=Set((Meta.Utf8,)),
                        )


# TODO incomplete
function _julia_eltype(ϕ::Meta.Field)
    if typeof(ϕ.dtype) ∈ CONTAINER_TYPES.primitive
        juliatype(ϕ.dtype)
    elseif typeof(ϕ.dtype) ∈ CONTAINER_TYPES.strings
        String
    elseif typeof(ϕ.dtype) ∈ CONTAINER_TYPES.lists
        Vector{julia_eltype(ϕ.children[1])}
    else
        throw(ArgumentError("unrecognized type $(ϕ.dtype)"))
    end
end
function _julia_eltype(ϕ::Meta.DictionaryEncoding)
    if typeof(ϕ.indexType) ∈ CONTAINER_TYPES.primitive
        juliatype(ϕ.indexType)
    else
        throw(ArgumentError("invalid dictionary index type $(ϕ.indexType)"))
    end
end
function _julia_eltype_nullable(ϕ::Union{Meta.Field,Meta.DictionaryEncoding})
    Union{_julia_eltype(ϕ),Missing}
end

"""
    julia_eltype(ϕ)

Gives the Julia element type of the Arrow `Field` metadata object.  For example, for an Arrow
`List<Int64>` this gives `Vector{Int64}` because the Julia object that is constructed to
represent this returns `Vector{Int64}` objects when indexed.
"""
julia_eltype(ϕ::Meta.Field) = ϕ.nullable ? _julia_eltype_nullable(ϕ) : _julia_eltype(ϕ)

Meta.juliatype(ϕ::Meta.Field) = AbstractVector{julia_eltype(ϕ)}

"""
    julia_keytype(ϕ)

Gives the Julia type of the keys of the Arrow `Field` object if it represents a dictionary
encoding.  As far as I know, this is always a `AbstractVector{<:Union{Integer,Missing}}`.
"""
function julia_keytype(ϕ::Meta.Field)
    ϕ.dictionary == nothing && return nothing
    if ϕ.nullable
        AbstractVector{_julia_eltype_nullable(ϕ.dictionary)}
    else
        AbstractVector{_julia_eltype(ϕ.dictionary)}
    end
end
#======================================================================================================
    \end{inferring schema}
======================================================================================================#
