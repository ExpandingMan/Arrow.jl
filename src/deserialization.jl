
struct Batch{H}
    header::H

    buffer::Vector{UInt8}
    body_start::Int
    body_length::Int
end

body_start(b::Batch) = b.body_start
body_length(b::Batch) = b.body_length
body_end(b::Batch) = body_start(b) + body_length(b) - 1

function Batch(m::Meta.Message, buf::Vector{UInt8}, i::Integer)
    Batch{typeof(m.header)}(m.header, buf, i, m.bodyLength)
end

# returns nothing if batch can't be read
"""
    Batch

Object which holds an arrow message along with the data buffer and pointers to the data.

## Constructors
```julia
Batch(m::Meta.Message, buf::Vector{UInt8}, i::Integer)
Batch(buf::Vector{UInt8}, rf::Integer=1, i::Integer=-1)
```

## Arguments
- `m`: An arrow message metadata object describing the batch.
- `buf`: A buffer to read from.
- `rf`: Index of the buffer `buf` to read the message `m` from.
- `i`: The start index (in `buf`) of the message body. If `i < 1`, this will be determined from `rf`.
"""
function Batch(buf::Vector{UInt8}, rf::Integer=1, i::Integer=-1)
    rf + 3 > length(buf) && return nothing
    l = reinterpret(Int32, buf[rf:(rf+3)])[1]
    l == 0 && return nothing
    m = readmessage(buf, rf+4)
    i < 1 && (i = rf+4+l)
    Batch(m, buf, i)
end

dictionary_id(b::Meta.DictionaryBatch) = b.id
dictionary_id(ϕ::Meta.DictionaryEncoding) = ϕ.id
function dictionary_id(ϕ::Meta.Field)
    ϕ.dictionary == nothing ? nothing : dictionary_id(ϕ.dictionary)
end
dictionary_id(sch::Meta.Schema, ϕ_idx::Integer) = dictionary_id(sch.fields[ϕ_idx])
dictionary_id(b::Batch{Meta.Schema}, ϕ_idx::Integer) = dictionary_id(b.header, ϕ_idx)
dictionary_id(b::Batch{Meta.DictionaryBatch}) = dictionary_id(b.header)

"""
    dict_field_idx(sch, id)

Return the Arrow metadata `Field` object from the Arrow metadata `Schema` with a dictionary
ID equal to `id`.
"""
function dict_field_idx(sch::Meta.Schema, id::Integer)
    findfirst(ϕ -> dictionary_id(ϕ) == id, sch.fields)
end
dict_field_idx(b::Batch{Meta.Schema}, id::Integer) = dict_field_idx(b.header, id)
function dict_field_idx(sch::Meta.Schema, b::Batch{Meta.DictionaryBatch})
    dict_field_idx(sch, dictionary_id(b))
end
function dict_field_idx(b1::Batch{Meta.Schema}, b2::Batch{Meta.DictionaryBatch})
    dict_field_idx(b1.header, b2)
end

function build(ϕ::Meta.Field, b::Batch, node_idx::Integer=1, buf_idx::Integer=1)
    build(ϕ, b.header, b.buffer, node_idx, buf_idx, body_start(b))
end
function build(sch::Meta.Schema, b::Batch, ϕ_idx::Integer, node_idx::Integer=1,
               buf_idx::Integer=1)
    build(sch.fields[ϕ_idx], b, node_idx, buf_idx)
end
function build(bsch::Batch{Meta.Schema}, b::Batch, ϕ_idx::Integer, node_idx::Integer=1,
               buf_idx::Integer=1)
    build(bsch.header, b, ϕ_idx, node_idx, buf_idx)
end

fieldname(sch::Meta.Schema, i::Integer) = Symbol(sch.fields[i].name)
fieldname(b::Batch{Meta.Schema}, i::Integer) = fieldname(b.header, i)
fieldnames(sch::Meta.Schema) = [fieldname(sch, i) for i ∈ 1:length(sch.fields)]
fieldnames(b::Batch{Meta.Schema}) = fieldnames(b.header)

struct BatchIterator{H}
    schema::Batch{Meta.Schema}
    batch::Batch{H}
end

fieldnames(bi::BatchIterator) = fieldnames(bi.schema)
function fieldname(bi::BatchIterator{Meta.DictionaryBatch}, i::Integer=1)
    fieldname(bi.schema, dict_field_idx(bi.schema, bi.batch))
end
fieldnames(bi::BatchIterator{Meta.DictionaryBatch}) = [fieldname(bi)]

dict_field_idx(bi::BatchIterator{Meta.DictionaryBatch}) = dict_field_idx(bi.schema, bi.batch)

# length of batch is number of columns
Base.length(bi::BatchIterator) = length(bi.schema.header.fields)

# dictionary batches always contain a single field
Base.length(bi::BatchIterator{Meta.DictionaryBatch}) = 1

function Base.iterate(bi::BatchIterator, state::Tuple=(1, 1, 1))
    state[1] > length(bi) && return nothing
    p, node_idx, buf_idx = build(bi.schema, bi.batch, state[1], state[2], state[3])
    p, (state[1]+1, node_idx, buf_idx)
end
function Base.iterate(bi::BatchIterator{Meta.DictionaryBatch}, state::Tuple=(1, 1, 1))
    state[1] > length(bi) && return nothing
    p, node_idx, buf_idx = build(bi.schema, bi.batch, dict_field_idx(bi), state[2], state[3])
    p, (state[1]+1, node_idx, buf_idx)
end

build(::Type{Tuple}, bi::BatchIterator) = tuple(bi...)
build(::Type{NamedTuple}, bi::BatchIterator) = (;(k=>p for (k,p) ∈ zip(fieldnames(bi), bi))...)
build(bi::BatchIterator) = build(NamedTuple, bi)


struct DataSet
    schema::Batch{Meta.Schema}
    dictionary_batches::Vector{Batch{Meta.DictionaryBatch}}
    record_batches::Vector{Batch{Meta.RecordBatch}}

    # TODO what about all the other types? (particularly tensors!!!)
end

function readbatches(buf::Vector{UInt8}, rf::AbstractVector{<:Integer},
                     i::AbstractVector{<:Integer}=fill(-1, length(rf)))
    batches = Vector{Batch}(undef, length(rf))
    for j ∈ 1:length(rf)
        batches[j] = Batch(buf, rf[j], i[j])
    end
    batches
end
function readbatches(buf::Vector{UInt8}, rf::Integer=1, max_batches::Integer=typemax(Int))
    batches = Vector{Batch}(undef, 0)
    while length(batches) < max_batches
        b = Batch(buf, rf)
        b == nothing && break
        push!(batches, b)
        rf = body_end(b) + 1
    end
    batches
end

function DataSet(batches::AbstractVector{<:Batch})
    sch = filter(b -> b isa Batch{Meta.Schema}, batches)
    if length(sch) > 1
        throw(ArgumentError("Multiple schemas found in batches, `Arrow.DataSet` must be "*
                            "constructed from a single schema."))
    end
    sch = first(sch)
    dicts = filter(b -> b isa Batch{Meta.DictionaryBatch}, batches)
    recs = filter(b -> b isa Batch{Meta.RecordBatch}, batches)
    DataSet(sch, dicts, recs)
end

function DataSet(buf::Vector{UInt8}, rf::AbstractVector{<:Integer},
                 i::AbstractVector{<:Integer}=fill(-1, length(rf)))
    DataSet(readbatches(buf, rf, i))
end
function DataSet(buf::Vector{UInt8}, rf::Integer=1, max_batches::Integer=typemax(Int))
    DataSet(readbatches(buf, rf, max_batches))
end

function BatchIterator(::Type{Meta.DictionaryBatch}, ds::DataSet, i::Integer)
    BatchIterator(ds.schema, ds.dictionary_batches[i])
end
function BatchIterator(::Type{Meta.RecordBatch}, ds::DataSet, i::Integer)
    BatchIterator(ds.schema, ds.record_batches[i])
end


# TODO do reading batches from IO!!!
# TODO next start on functions that build entire dataset all at once
