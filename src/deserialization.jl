
abstract type AbstractBatch end

bodystart(b::AbstractBatch) = b.body_start
bodylength(b::AbstractBatch) = b.body_length
bodyend(b::AbstractBatch) = bodystart(b) + bodylength(b) - 1

hasbody(::AbstractBatch) = true

reset!(b::AbstractBatch) = b

struct EmptyBatch{H} <: AbstractBatch
    header::H

    # this is kept only to keep track of location in the buffer
    body_start::Int
    body_length::Int
end

batch(m, blen::Integer, buf::Vector{UInt8}, i::Integer) = EmptyBatch(m, i, blen)

hasbody(::EmptyBatch) = false


mutable struct RecordBatch <: AbstractBatch
    header::Meta.RecordBatch

    buffer::Vector{UInt8}
    body_start::Int
    body_length::Int

    node_idx::Int
    buf_idx::Int
end

setnodeindex!(rb::RecordBatch, idx::Integer=1) = (rb.node_idx = idx)
setbufferindex!(rb::RecordBatch, idx::Integer=1) = (rb.buf_idx = idx)

reset!(rb::RecordBatch) = (setnodeindex!(rb); setbufferindex!(rb); rb)

function RecordBatch(m::Meta.RecordBatch, blen::Integer, buf::Vector{UInt8}, i::Integer)
    RecordBatch(m, buf, i, blen, 1, 1)
end
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

function buildnext!(::typeof(offsets), rb::RecordBatch)
    n = getnode(rb)
    b = getbuffer!(rb)
    Primitive{DefaultOffset}(rb.buffer, bodystart(rb)+b.offset, b.length ÷ sizeof(DefaultOffset))
end

# NOTE: first node gets skipped only for strings for some unholy reason
function buildnext!(::Type{<:AbstractVector{T}}, rb::RecordBatch, skipnode::Bool=true) where {T}
    o = buildnext!(offsets, rb)
    skipnode && getnode!(rb)
    v = buildnext!(T, rb)
    List{eltype(v),typeof(v)}(v, o)
end

function buildnext!(::Type{String}, rb::RecordBatch)
    l = buildnext!(Vector{UInt8}, rb, false)
    ConvertVector{String,typeof(l)}(l)
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
    BitPrimitive(Primitive{UInt8}(rb.buffer, bodystart(rb)+b.offset, b.length), n.length)
end

function buildnext!(::Type{Union{T,Missing}}, rb::RecordBatch) where {T}
    b = buildnext!(bitmask, rb)
    v = buildnext!(T, rb)
    isnothing(b) ? v : NullableVector{T,typeof(v)}(v, b)
end


mutable struct DictionaryBatch <: AbstractBatch
    header::Meta.DictionaryBatch
    record_batch::RecordBatch

    buffer::Vector{UInt8}
    body_start::Int
    body_length::Int
end

function DictionaryBatch(m::Meta.DictionaryBatch, blen::Integer, buf::Vector{UInt8}, i::Integer)
    # TODO is it ok to pass RecordBatch constructed with same body_start?
    DictionaryBatch(m, RecordBatch(m.data, blen, buf, i), buf, i, blen)
end
batch(m::Meta.DictionaryBatch, blength, buf::Vector{UInt8}, i::Integer) = DictionaryBatch(m, buf, i)

dictionaryid(b::Meta.DictionaryBatch) = b.id
dictionaryid(ϕ::Meta.DictionaryEncoding) = ϕ.id
dictionaryid(ϕ::Meta.Field) = ϕ.dictionary == nothing ? nothing : dictionaryid(ϕ.dictionary)
dictionaryid(b::DictionaryBatch) = dictionaryid(b.header)

nnodes(db::DictionaryBatch) = nnodes(db.record_batch)
nbuffers(db::DictionaryBatch) = nbuffers(db.record_batch)

getnode(db::DictionaryBatch, idx) = getnode(db.record_batch, idx)
getbuffer(db::DictionaryBatch, idx) = getnode(db.record_batch, idx)

nodeindex!(db::DictionaryBatch, δ::Integer=1) = nodeindex!(db.record_batch, δ)
bufferindex!(db::DictionaryBatch, δ::Integer=1) = nodeindex!(db.record_batch, δ)


batch(m::Meta.Message, buf::Vector{UInt8}, i::Integer) = batch(m.header, m.bodyLength, buf, i)

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


mutable struct Column
    header::Meta.Field
    batches::Vector{AbstractBatch}
    node_idx::Vector{Union{Int,Nothing}}  # start position of nodes
    buf_idx::Vector{Union{Int,Nothing}}  # start position of buffers
end

Meta.juliatype(c::Column) = Meta.juliatype(c.header)

reset!(c::Column) = (foreach(reset!, c.batches); c)

name(c::Column) = Symbol(c.header.name)
isnullable(c::Column) = c.header.nullable
batches(c::Column) = c.batches
nbatches(c::Column) = length(batches(c))

getnodeindices(c::Column) = [c.batches[i].node_idx for i ∈ 1:nbatches(c)]
getbufferindices(c::Column) = [c.batches[i].buf_idx for i ∈ 1:nbatches(c)]

"""
    setfirstcolumn!(c::Column)

Sets the indices appropriate for the first column in a schema.
"""
function setfirstcolumn!(c::Column)
    c.node_idx = fill(1, length(c.batches))
    c.buf_idx = fill(1, length(c.batches))
    c
end

function Column(sch::Meta.Schema, idx::Integer, batches::AbstractVector{<:AbstractBatch})
    Column(sch.fields[idx], batches, fill(nothing, length(batches)), fill(nothing, length(batches)))
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

build(c::Column) = nbatches == 1 ? build(c, 1) : Vcat((build(c, i) for i ∈ 1:nbatches(c))...)

function build(c::Column, i::Integer)
    n, b = c.node_idx[i], c.buf_idx[i]
    if isnothing(n) || isnothing(b)
        throw(ErrorException("tried to build uninitialized column `$(name(c))`"))
    end
    setnodeindex!(c.batches[i], c.node_idx[i])
    setbufferindex!(c.batches[i], c.buf_idx[i])
    build(c.header, c.batches[i])
end

build(ϕ::Meta.Field, rb::RecordBatch) = build(juliatype(ϕ), ϕ, rb)

build(::Type{AbstractVector{T}}, ϕ::Meta.Field, rb::RecordBatch) where{T} = buildnext!(T, rb)

# TODO now need to do fields with children

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


# TODO not settled on name of this yet
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
    setfirstcolumn!(first(cols))
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
        t.columns[i+1].node_idx = getnodeindices(t.columns[i])
        t.columns[i+1].buf_idx = getbufferindices(t.columns[i])
    end
    p
end
build(t::Table, s::Symbol) = build(t, findfirst(c -> name(c) == s, columns(t)))

build(t::Table) = (;(name(column(t, i))=>build(t, i) for i ∈ 1:ncolumns(t))...)

#============================================================================================
    \begin{from RecordBatch}

    # TODO do we want to keep this with something that tracks the indices of everything?
============================================================================================#
function primitive(::Type{T}, b::Meta.Buffer, buf::Vector{UInt8}, ℓ::Integer, i::Integer=1
                  ) where {T}
    Primitive{T}(buf, i + b.offset, ℓ)
end
function primitive(::Type{T}, ϕn::Meta.FieldNode, b::Meta.Buffer, buf::Vector{UInt8},
                   i::Integer=1) where {T}
    primitive(T, b, buf, ϕn.length, i)
end
function primitive(::Type{T}, rb::Meta.RecordBatch, buf::Vector{UInt8},
                   node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1) where {T}
    primitive(T, rb.nodes[node_idx], rb.buffers[buf_idx], buf, i)
end

function bitprimitive(ϕn::Meta.FieldNode, b::Meta.Buffer, buf::Vector{UInt8}, i::Integer=1)
    BitPrimitive(primitive(UInt8, ϕn, b, buf, i), ϕn.length)
end
function bitprimitive(rb::Meta.RecordBatch, buf::Vector{UInt8},
                      node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    bitprimitive(rb.nodes[node_idx], rb.buffers[buf_idx], buf, i)
end

function bitmask(ϕn::Meta.FieldNode, b::Meta.Buffer, buf::Vector{UInt8}, i::Integer=1)
    bitprimitive(ϕn, b, buf, i)
end
function bitmask(rb::Meta.RecordBatch, buf::Vector{UInt8},
                 node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    bitmask(rb.nodes[node_idx], rb.buffers[buf_idx], buf, i)
end

function offsets(ϕn::Meta.FieldNode, b::Meta.Buffer, buf::Vector{UInt8}, i::Integer=1)
    Primitive{DefaultOffset}(buf, i + b.offset, ϕn.length+1)
end
function offsets(rb::Meta.RecordBatch, buf::Vector{UInt8},
                 node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    offsets(rb.nodes[node_idx], rb.buffers[buf_idx], buf, i)
end

function _check_empty_buffer(rb::Meta.RecordBatch, node_idx::Integer, buf_idx::Integer)
    rb.nodes[node_idx].null_count == 0 && rb.buffers[buf_idx].length == 0
end

#=--------------------------------------------------------------------------------------
NOTE: the ordering of the buffers is canonical, and can be found
 [here](https://arrow.apache.org/docs/format/Columnar.html#buffer-alignment-and-padding)
--------------------------------------------------------------------------------------=#
function build(::Type{AbstractVector{T}}, rb::Meta.RecordBatch, buf::Vector{UInt8},
               node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1) where {T}
    primitive(T, rb, buf, node_idx, buf_idx, i), node_idx+1, buf_idx+1
end
function build(::Type{AbstractVector{Union{T,Missing}}}, rb::Meta.RecordBatch,
               buf::Vector{UInt8}, node_idx::Integer=1, buf_idx::Integer=1,
               i::Integer=1) where {T}
    # handle cases where schema says nullable, but mask is missing
    if _check_empty_buffer(rb, node_idx, buf_idx)
        return build(AbstractVector{T}, rb, buf, node_idx, buf_idx+1, i)
    end
    b = bitmask(rb, buf, node_idx, buf_idx, i)
    buf_idx += 1
    v, node_idx, buf_idx = build(AbstractVector{T}, rb, buf, node_idx, buf_idx, i)
    NullableVector{T,typeof(v)}(v, b), node_idx, buf_idx
end
function build(::Type{AbstractVector{Vector{T}}}, rb::Meta.RecordBatch,
               buf::Vector{UInt8}, node_idx::Integer=1, buf_idx::Integer=1,
               i::Integer=1) where {T}
    o = offsets(rb, buf, node_idx, buf_idx, i)
    node_idx += 1
    buf_idx += 1
    v, node_idx, buf_idx = build(AbstractVector{T}, rb, buf, node_idx, buf_idx, i)
    List{eltype(v),typeof(v)}(v, o), node_idx, buf_idx
end
function build(::Type{AbstractVector{Union{Vector{T},Missing}}}, rb::Meta.RecordBatch,
               buf::Vector{UInt8}, node_idx::Integer=1, buf_idx::Integer=1,
               i::Integer=1) where {T}
    if _check_empty_buffer(rb, node_idx, buf_idx)
        return build(AbstractVector{Vector{T}}, rb, buf, node_idx, buf_idx+1, i)
    end
    b = bitmask(rb, buf, node_idx, buf_idx, i)
    buf_idx += 1
    l, node_idx, buf_idx = build(AbstractVector{Vector{T}}, rb, buf, node_idx, buf_idx, i)
    NullableVector{bare_eltype(l),typeof(l)}(l, b), node_idx, buf_idx
end

function _string_list(rb::Meta.RecordBatch, buf::Vector{UInt8}, node_idx::Integer=1,
                      buf_idx::Integer=1, i::Integer=1)
    o = offsets(rb, buf, node_idx, buf_idx, i)
    buf_idx += 1
    # note that the creation of this primitive also requires special handling
    v = primitive(UInt8, rb.buffers[buf_idx], buf, rb.buffers[buf_idx].length, i)
    List{eltype(v),typeof(v)}(v, o)
end
# NOTE: they left us no choice but to have special methods for strings
function build(::Type{AbstractVector{String}}, rb::Meta.RecordBatch, buf::Vector{UInt8},
               node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    l = _string_list(rb, buf, node_idx, buf_idx, i)
    ConvertVector{String,typeof(l)}(l), node_idx+1, buf_idx+2
end
function build(::Type{AbstractVector{Union{String,Missing}}}, rb::Meta.RecordBatch,
               buf::Vector{UInt8}, node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    if _check_empty_buffer(rb, node_idx, buf_idx)
        return build(AbstractVector{String}, rb, buf, node_idx, buf_idx+1, i)
    end
    b = bitmask(rb, buf, node_idx, buf_idx, i)
    buf_idx += 1
    l = _string_list(rb, buf, node_idx, buf_idx, i)
    l = NullableVector{Vector{UInt8},typeof(l)}(l, b)
    ConvertVector{Union{String,Missing},typeof(l)}(l), node_idx+1, buf_idx+2
end
#============================================================================================
    \end{from RecordBatch}
============================================================================================#

#============================================================================================
    \begin{build from schema field}
============================================================================================#
const CONTAINER_TYPES = (primitive=Union{Meta.Int_,Meta.FloatingPoint},
                         lists=Meta.List,
                         strings=Meta.Utf8,
                        )

# TODO incomplete
function _julia_eltype(ϕ::Meta.Field)
    if typeof(ϕ.dtype) <: CONTAINER_TYPES.primitive
        juliatype(ϕ.dtype)
    elseif typeof(ϕ.dtype) <: CONTAINER_TYPES.strings
        String
    elseif typeof(ϕ.dtype) <: CONTAINER_TYPES.lists
        Vector{julia_eltype(ϕ.children[1])}
    else
        throw(ArgumentError("unrecognized type $(ϕ.dtype)"))
    end
end
function _julia_eltype(ϕ::Meta.DictionaryEncoding)
    if typeof(ϕ.indexType) <: CONTAINER_TYPES.primitive
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

"""
    julia_valtype(ϕ)

Gives the Julia values type of the Arrow `Field` metadata object.  The constructed object is
typically a subtype of this (though there is an exception because of how the arrow standard
decided to handle nullables).
"""
julia_valtype(ϕ::Meta.Field) = AbstractVector{julia_eltype(ϕ)}

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

"""
    juliatype(ϕ)

Returns the Julia type corresponding to the Arrow `Field` metadata given by `ϕ`.

For dictionary fields, this returns the index type.
"""
Meta.juliatype(ϕ::Meta.Field) = julia_valtype(ϕ)

"""
    build

This function takes as its arguments Arrow metadata, which it then uses to call other methods
(with Julia metadata) for constructing arrays.
"""
function build(ϕ::Meta.Field, rb::Meta.RecordBatch, buf::Vector{UInt8}, node_idx::Integer=1,
               buf_idx::Integer=1, i::Integer=1)
    build(juliatype(ϕ), rb, buf, node_idx, buf_idx, i)
end
function build(ϕ::Meta.Field, rb::Meta.DictionaryBatch, buf::Vector{UInt8},
               node_idx::Integer=1, buf_idx::Integer=1, i::Integer=1)
    build(juliatype(ϕ), rb.data, buf, node_idx, buf_idx, i)
end
#============================================================================================
    \end{build from schema field}
============================================================================================#
