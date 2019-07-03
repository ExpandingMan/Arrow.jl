
# TODO this will probably have to change significantly to support dictionaries and tensors
struct Batch
    schema::Meta.Schema
    record_batch::Meta.RecordBatch

    buffer::Vector{UInt8}
    body_start::Int
    body_length::Int
end


function Batch(sch::Meta.Schema, buf::Vector{UInt8}, i::Integer=1)
    l = reinterpret(Int32, buf[i:(i+3)])[1]
    m = readmessage(buf, i+4)
    Batch(sch, m.header, buf, i+4+l, m.bodyLength)
end

function build(ϕ::Meta.Field, b::Batch, node_idx::Integer=1, buf_idx::Integer=1)
    build(juliatype(ϕ), b.record_batch, b.buffer, node_idx, buf_idx, b.body_start)
end
function build(b::Batch, ϕ_idx::Integer, node_idx::Integer=1, buf_idx::Integer=1)
    build(b.schema.fields[ϕ_idx], b, node_idx, buf_idx)
end

fieldname(b::Batch, i::Integer) = Symbol(b.schema.fields[i].name)
fieldnames(b::Batch) = [fieldname(b, i) for i ∈ 1:length(b.schema.fields)]

# length of batch is number of columns
Base.length(b::Batch) = length(b.schema.fields)
function Base.iterate(b::Batch, state::Tuple=(1, 1, 1))
    state[1] > length(b.schema.fields) && return nothing
    p, node_idx, buf_idx = build(b, state[1], state[2], state[3])
    p, (state[1]+1, node_idx, buf_idx)
end

build(::Type{Tuple}, b::Batch) = tuple(b...)
build(::Type{NamedTuple}, b::Batch) = (;(k=>p for (k, p) ∈ zip(fieldnames(b), b))...)
build(b::Batch) = build(NamedTuple, b)
