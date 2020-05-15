
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
    \begin{more metadata constructors}
======================================================================================================#
function Meta.Schema(sch::Tables.Schema; custom_metadata=Dict())
    Meta.Schema(Meta.EndiannessLittle,
                # TODO need way to put more of the metadata in here eventually
                [Meta.Field(n, dtype) for (n, dtype) ∈ zip(sch.names, sch.types)],
                [Meta.KeyValue(kv) for kv ∈ custom_metadata])
end

function Meta.RecordBatch(v::AbstractVector, i::Integer=1)
    Meta.RecordBatch(length(v), metanodes(v), metabuffers(v, i))
end

# TODO need to handle child nodes!
metanodes(v::AbstractVector) = [Meta.FieldNode(length(v), count(ismissing, v))]
function metabuffers(v::AbstractVector, i::Integer=1)
    bufs = Vector{Meta.Buffer}(undef, 0)
    for (ctype, c) ∈ components(v)
        n = nbytes(c)
        push!(bufs, Meta.Buffer(i-1, n))
        i += n
    end
    bufs
end
#======================================================================================================
    \end{more metadata constructors}
======================================================================================================#
