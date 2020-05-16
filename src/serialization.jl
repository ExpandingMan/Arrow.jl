
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

writemeta!(rb::RecordBatch) = FB.serialize(rb.buffer, rb.header)

function write!(rb::RecordBatch, v::AbstractVector)
    s = 0
    for (ctype, c) ∈ components(v)
        s += write!(rb, c)
    end
    s
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

function Meta.RecordBatch(vs, nodes::AbstractVector{Meta.FieldNode}=metadata(Meta.FieldNode, vs),
                          mbufs::AbstractVector{Meta.Buffer}=metadata(Meta.Buffer, vs))
    # again, being careful to work on NamedTuple
    l = length(first(vs))
    for v ∈ vs
        if length(v) ≠ l
            throw(ArgumentError("can only construct a RecordBatch from equal length vectors"))
        end
    end
    Meta.RecordBatch(l, nodes, mbufs)
end

# TODO need to handle child nodes!
Meta.FieldNode(v::AbstractVector) = Meta.FieldNode(length(v), count(ismissing, v))
Meta.Buffer(v::AbstractVector, o::Integer, n::Integer=nbytes(v)) = Meta.Buffer(o-1, n)

# using the below form so it works on NamedTuple
metadata(::Type{Meta.FieldNode}, vs) = collect(Meta.FieldNode(v) for v ∈ vs)
metadata!(ϕs::AbstractVector{Meta.FieldNode}, v::AbstractVector) = push!(ϕs, Meta.FieldNode(v))

function bodyend(mbufs::AbstractVector{Meta.Buffer}, o::Integer=1)
    isempty(mbufs) && return o
    lbuf = last(mbufs)
    lbuf.offset + lbuf.length + o
end

function metadata!(mbufs::AbstractVector{Meta.Buffer}, ::typeof(components),
                   v::AbstractVector, o::Integer)
    push!(mbufs, Meta.Buffer(v, o))
end
function metadata!(mbufs::AbstractVector{Meta.Buffer}, ::typeof(components),
                   v::AbstractVector)
    metadata!(mbufs, v, bodyend(mbufs))
end
function metadata!(mbufs::AbstractVector{Meta.Buffer}, v::AbstractVector, o::Integer)
    for (i, (ctype, c)) ∈ enumerate(components(v))
        be = i == 1 ? o : bodyend(mbufs)
        metadata!(mbufs, components, c, be)
    end
    mbufs
end
function metadata(::Type{Meta.Buffer}, vs, o::Integer=1)
    mbufs = Vector{Meta.Buffer}(undef, 0)
    for (i, v) ∈ enumerate(vs)
        be = i == 1 ? o : bodyend(mbufs)
        metadata!(mbufs, v, be)
    end
    mbufs
end
#======================================================================================================
    \end{batch metadata}
======================================================================================================#
