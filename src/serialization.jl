
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
#======================================================================================================
    \end{column meta}
======================================================================================================#

#======================================================================================================
    \begin{schemas}
======================================================================================================#
function Meta.Schema(sch::Tables.Schema; custom_metadata=Dict())
    Meta.Schema(Meta.EndiannessLittle,
                # TODO need way to put more of the metadata in here eventually
                [Meta.Field(n, dtype) for (n, dtype) ∈ zip(sch.names, sch.types)],
                [Meta.KeyValue(kv) for kv ∈ custom_metadata])
end
#======================================================================================================
    \end{schemas}
======================================================================================================#
