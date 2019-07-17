
struct Batch{H}
    header::H

    buffer::Vector{UInt8}
    body_start::Int
    body_length::Int
end

body_start(b::Batch) = b.body_start
body_length(b::Batch) = b.body_length
body_end(b::Batch) = body_start(b) + body_length(b) - 1

"""
    Batch

Object which holds an arrow message along with the data buffer and pointers to the data.

## Constructors
```julia
Batch(m::Meta.Message, buf::Vector{UInt8}, i::Integer)
```
"""
function Batch(m::Meta.Message, buf::Vector{UInt8}, i::Integer)
    Batch{typeof(m.header)}(m.header, buf, i, m.bodyLength)
end

"""
    readmessage_length(io)

Arrow messages are prepended with the message header length as an `Int32`.  This function returns a
tuple `l, m` where `l` is that length and `m` is the message after reading them from the `IO`.

If the end of the file is reached `(0, nothing)` will be returned.
"""
function readmessage_length(io::IO)
    eof(io) && return (0, nothing)
    l = read(io, Int32)
    eof(io) && return (0, nothing)  # should we give warning for this
    m = readmessage(io, l)
    l, m
end

# returns nothing if batch can't be read (consider if this should be renamed `batch` because of that)
"""
    batch(buf::Vector{UInt8}, rf::Integer=1, i::Integer=-1, databuf::Vector{UInt8}=buf)

Read in a batch from a buffer or IO stream.

## Arguments
- `m`: An arrow message metadata object describing the batch.
- `buf`: A buffer to read from.
- `databuf`: Buffer containing actual data (body).
- `rf`: Index of the buffer `buf` to read the message `m` from.
- `i`: The start index (in `buf`) of the message body. If `i < 1`, this will be determined from `rf`.
"""
function batch(buf::Vector{UInt8}, rf::Integer=1, i::Integer=-1, databuf::Vector{UInt8}=buf)
    rf + 3 > length(buf) && return nothing
    l = reinterpret(Int32, buf[rf:(rf+3)])[1]
    l == 0 && return nothing
    m = readmessage(buf, rf+4)
    i < 1 && (i = rf+4+l)
    Batch(m, databuf, i)
end
function batch(io::IO, buf::Vector{UInt8}, i::Integer=1)
    l, m = readmessage_length(io)
    l == 0 && return nothing
    Batch(m, buf, i)
end
function batch(io::IO, dataio::IO=io, data_skip::Integer=0)
    l, m = readmessage_length(io)
    l == 0 && return nothing
    skip(dataio, data_skip)
    buf = read(dataio, m.bodyLength)
    Batch(m, buf, 1)
end

"""
    dictionary_id

Gets the dictionary ID associated with the field or batch.
"""
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

"""
    fieldname(ϕ::Meta.Field)
    fieldname(schema, field_idx)

Get the name of the column described by the field metadata.
"""
fieldname(ϕ::Meta.Field) = Symbol(ϕ.name)
fieldname(sch::Meta.Schema, i::Integer) = fieldname(sch.fields[i])
fieldname(b::Batch{Meta.Schema}, i::Integer) = fieldname(b.header, i)
fieldnames(sch::Meta.Schema) = [fieldname(sch, i) for i ∈ 1:length(sch.fields)]
fieldnames(b::Batch{Meta.Schema}) = fieldnames(b.header)

struct BatchIterator{H}
    schema::Batch{Meta.Schema}
    batch::Batch{H}
end

"""
    fieldnames(bi::BatchIterator)
    fieldnames(schema)

Get the names of all columns.
"""
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

    dictionary_data::Vector{Any}
    record_data::Vector{Any}

    # TODO what about all the other types? (particularly tensors!!!)
end

"""
    getcolumnmeta

Get the column names of the `DataSet`.
"""
getcolumnmeta(ds::DataSet) = ds.schema.header.fields
getcolumnmeta(ds::DataSet, i::Integer) = getcolumnmeta(ds)[i]
getcolumnmeta(ds::DataSet, name::Symbol) = findfirst(ϕ -> fieldname(ϕ) == name,
                                                     getcolumnmeta(ds))
ncolumns(ds::DataSet) = length(ds.schema.header.fields)

"""
    readbatches

Read all batches from a buffer or IO stream.  The reading will be attempted sequentially and
will terminate when the end of the stream or buffer or a `0` length specifier is encountered.
"""
function readbatches(buf::Vector{UInt8}, rf::AbstractVector{<:Integer},
                     i::AbstractVector{<:Integer}=fill(-1, length(rf)), databuf::Vector{UInt8}=buf)
    batches = Vector{Batch}(undef, length(rf))
    for j ∈ 1:length(rf)
        batches[j] = batch(buf, rf[j], i[j], databuf)
    end
    batches
end
function readbatches(buf::Vector{UInt8}, rf::Integer=1, max_batches::Integer=typemax(Int))
    batches = Vector{Batch}(undef, 0)
    while length(batches) < max_batches
        b = batch(buf, rf)
        b == nothing && break
        push!(batches, b)
        rf = body_end(b) + 1
    end
    batches
end
function readbatches(io::IO, dataio::IO=io, data_skip::Integer=0,
                     max_batches::Integer=typemax(Int))
    batches = Vector{Batch}(undef, 0)
    while length(batches) < max_batches
        b = batch(io, dataio, data_skip)
        b == nothing && break
        push!(batches, b)
    end
    batches
end

function DataSet(batches::AbstractVector{<:Batch}; build_data::Bool=true)
    if isempty(batches)
        throw(ArgumentError("no batches provided, unable to build dataset"))
    end
    sch = filter(b -> b isa Batch{Meta.Schema}, batches)
    if length(sch) > 1
        throw(ArgumentError("Multiple schemas found in batches, `Arrow.DataSet` must be "*
                            "constructed from a single schema."))
    end
    sch = first(sch)
    dicts = filter(b -> b isa Batch{Meta.DictionaryBatch}, batches)
    recs = filter(b -> b isa Batch{Meta.RecordBatch}, batches)
    ds = DataSet(sch, dicts, recs, [], [])
    build_data && build!(ds)
    ds
end

function DataSet(buf::Vector{UInt8}, rf::AbstractVector{<:Integer},
                 i::AbstractVector{<:Integer}=fill(-1, length(rf)),
                 databuf::Vector{UInt8}=buf; build_data::Bool=true)
    DataSet(readbatches(buf, rf, i, databuf), build_data=build_data)
end
function DataSet(buf::Vector{UInt8}, rf::Integer=1, max_batches::Integer=typemax(Int);
                 build_data::Bool=true)
    DataSet(readbatches(buf, rf, max_batches), build_data=build_data)
end
function DataSet(io::IO, dataio::IO=io, data_skip::Integer=0,
                 max_batches::Integer=typemax(Int); build_data::Bool=true)
    DataSet(readbatches(io, dataio, data_skip, max_batches), build_data=build_data)
end

function BatchIterator(::Type{Meta.DictionaryBatch}, ds::DataSet, i::Integer)
    BatchIterator(ds.schema, ds.dictionary_batches[i])
end
function BatchIterator(::Type{Meta.RecordBatch}, ds::DataSet, i::Integer)
    BatchIterator(ds.schema, ds.record_batches[i])
end

function build_dictionaries!(ds::DataSet)
    length(ds.dictionary_data) > 0 && return ds.dictionary_data
    for i ∈ 1:length(ds.dictionary_batches)
        bi = BatchIterator(Meta.DictionaryBatch, ds, i)
        push!(ds.dictionary_data, build(NamedTuple, bi))
    end
    ds.dictionary_data
end
function build_records!(ds::DataSet)
    length(ds.record_data) > 0 && return ds.record_data
    for i ∈ 1:length(ds.record_batches)
        bi = BatchIterator(Meta.RecordBatch, ds, i)
        push!(ds.record_data, build(NamedTuple, bi))
    end
    ds.record_data
end


build!(ds::DataSet) = (build_dictionaries!(ds); build_records!(ds))


"""
    assemble_keys(ds::DataSet, ϕ::Meta.Field)

Build the full Arrow view object associated with field `ϕ` in dataset `ds`.

For dictionaries, this will build the keys only.
"""
function assemble_keys(ds::DataSet, ϕ::Meta.Field)
    dtype = ϕ.dictionary == nothing ? julia_eltype(ϕ) : _julia_eltype(ϕ.dictionary)
    length(ds.record_data) == 0 && return Vector{dtype}()
    if length(ds.record_data) == 1
        getproperty(ds.record_data[1], filedname(ϕ))
    else
        Vcat((getproperty(ds.record_data[i], fieldname(ϕ))
              for i ∈ 1:length(ds.record_data))...)
    end
end

"""
    assemble_values(ds::DataSet, ϕ::Meta.Field)

Build the full Arrow view object associated with a dictionary field.
"""
function assemble_values(ds::DataSet, ϕ::Meta.Field)
    length(ds.dictionary_data) == 0 && return Vector{julia_eltype(ϕ)}()
    if ϕ.dictionary == nothing
        return assemble_keys(ds, ϕ)
    end
    idx = findfirst(b -> dictionary_id(b) == ϕ.dictionary.id, ds.dictionary_batches)
    if idx == nothing
        throw(ErrorException("could not find values for dictionary ID $(ϕ.dictionary.id)"))
    end
    getproperty(ds.dictionary_data[idx], fieldname(ϕ))
end
function assemble(ds::DataSet, ϕ::Meta.Field)
    if ϕ.dictionary == nothing
        assemble_keys(ds, ϕ)
    else
        DictVector(assemble_keys(ds, ϕ), assemble_values(ds, ϕ))
    end
end
assemble(ds::DataSet, i::Integer) = assemble(ds, getcolumnmeta(ds, i))
assemble(ds::DataSet, name::Symbol) = assemble(ds, getcolumnmeta(ds, name))
function assemble(::Type{Tuple}, ds::DataSet)
    tuple((assemble(ds, ϕ) for ϕ ∈ getcolumnmeta(ds))...)
end
function assemble(::Type{NamedTuple}, ds::DataSet)
    (;(fieldname(ϕ)=>assemble(ds, ϕ) for ϕ ∈ getcolumnmeta(ds))...)
end

"""
    assemble(ds)

Assemble a named tuple the keys of which are the column names and the values of which are the
arrays of the `DataSet` `ds`.
"""
assemble(ds::DataSet) = assemble(NamedTuple, ds)
