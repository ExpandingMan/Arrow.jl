
struct Batch{H}
    schema::Meta.Schema
    header::H

    buffer::Vector{UInt8}
    body_start::Int
    body_length::Int
end

function Batch(sch::Meta.Schema, m::Meta.Message, buf::Vector{UInt8}, l::Integer,
               i::Integer=1)
    Batch{typeof(m.header)}(sch, m.header, buf, i+4+l, m.bodyLength)
end
function Batch(sch::Meta.Schema, buf::Vector{UInt8}, i::Integer=1)
    l = reinterpret(Int32, buf[i:(i+3)])[1]
    m = readmessage(buf, i+4)
    Batch(sch, m, buf, l, i)
end

dictionary_id(b::Meta.DictionaryBatch) = b.id
dictionary_id(ϕ::Meta.DictionaryEncoding) = ϕ.id
function dictionary_id(ϕ::Meta.Field)
    ϕ.dictionary == nothing ? nothing : dictionary_id(ϕ.dictionary)
end
dictionary_id(sch::Meta.Schema, ϕ_idx::Integer) = dictionary_id(sch.fields[ϕ_idx])
dictionary_id(b::Batch{Meta.DictionaryBatch}) = dictionary_id(b.header)

"""
    dict_field_idx(sch, id)

Return the Arrow metadata `Field` object from the Arrow metadata `Schema` with a dictionary
ID equal to `id`.
"""
function dict_field_idx(sch::Meta.Schema, id::Integer)
    findfirst(ϕ -> dictionary_id(ϕ) == id, sch.fields)
end
function dict_field_idx(b::Batch{Meta.DictionaryBatch})
    dict_field_idx(b.schema, dictionary_id(b))
end

function build(ϕ::Meta.Field, b::Batch, node_idx::Integer=1, buf_idx::Integer=1)
    build(ϕ, b.header, b.buffer, node_idx, buf_idx, b.body_start)
end
function build(b::Batch, ϕ_idx::Integer, node_idx::Integer=1, buf_idx::Integer=1)
    build(b.schema.fields[ϕ_idx], b, node_idx, buf_idx)
end

fieldname(b::Batch, i::Integer) = Symbol(b.schema.fields[i].name)
fieldnames(b::Batch) = [fieldname(b, i) for i ∈ 1:length(b.schema.fields)]

# length of batch is number of columns
Base.length(b::Batch) = length(b.schema.fields)

# dictionary batches always contain a single field
Base.length(b::Batch{Meta.DictionaryBatch}) = 1

function Base.iterate(b::Batch, state::Tuple=(1, 1, 1))
    state[1] > length(b) && return nothing
    p, node_idx, buf_idx = build(b, state[1], state[2], state[3])
    p, (state[1]+1, node_idx, buf_idx)
end
function Base.iterate(b::Batch{Meta.DictionaryBatch}, state::Tuple=(1, 1, 1))
    state[1] > length(b) && return nothing
    p, node_idx, buf_idx = build(b, dict_field_idx(b), state[2], state[3])
    p, (state[1]+1, node_idx, buf_idx)
end

build(::Type{Tuple}, b::Batch) = tuple(b...)
# TODO fix names for dicts
build(::Type{NamedTuple}, b::Batch) = (;(k=>p for (k, p) ∈ zip(fieldnames(b), b))...)
build(b::Batch) = build(NamedTuple, b)
