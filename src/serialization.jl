
# NOTE: a lot of this is slow right now, but 1.5 should drastically speed things up because
# of all the allocations from views

#======================================================================================================
    \begin{data serialization}
======================================================================================================#
write!(io::IO, ::Type{Primitive}, v::AbstractVector) = writepadded(io, v)

function write!(io::IO, ::Type{BitVector}, v::AbstractVector)
    bitpack!(io, v)
    bitpackedbytes(length(v))
end

write!(io::IO, ::typeof(bitmask), v::AbstractVector) = (bitpack!(io, .!ismissing.(v)); io)

function write!(io::IO, ::typeof(offsets), v::AbstractVector)
    last = zero(DefaultOffset)
    s = write(io, last)
    for j ∈ 2:(length(v)+1)
        next = DefaultOffset(offlength(v[j-1]) + last)
        s += write(io, next)
        last = next
    end
    p = paddinglength(s)
    skip(io, p)
    s + p
end

# TODO need to make sure we consistently catch the missings in the below

write!(io::IO, ::typeof(values), v::AbstractVector) = write!(io, Primitive, v)
function write!(io::IO, ::typeof(values), v::AbstractVector{Union{T,Missing}}) where {T}
    s = 0
    for x ∈ v
        s += if ismissing(x)
            skip(io, sizeof(eltype(v)))
            sizeof(eltype(v))
        else
            write(io, x)
        end
    end
    p = paddinglength(s)
    skip(io, p)
    s + p
end
write!(io::IO, ::typeof(values), v::AbstractVector{<:AbstractVector}) = write!(io, values(v))
function write!(io::IO, ::typeof(values), v::AbstractVector{<:Union{AbstractVector,Missing}})
    write!(io, bitmask, v) + write!(io, values(v))
end

write!(io::IO, v::AbstractVector) = write!(io, values, v)
function write!(io::IO, v::AbstractVector{Types.Nullable{T}}) where {T}
    write!(io, bitmask, v) + write!(io, values, v)
end
function write!(io::IO, v::AbstractVector{<:Types.List})
    write!(io, offsets, v) + write!(io, values, v)
end
function write!(io::IO, v::AbstractVector{<:Types.Strings})
    write!(io, offsets, v) + write!(io, values, codeunits.(v))
end
#======================================================================================================
    \end{data serialization}
======================================================================================================#

#======================================================================================================
    \begin{arrow format for individual vectors}
======================================================================================================#
arrow(v::AbstractVector) = Primitive(v)
arrow(v::AbstractVector{Bool}) = BitVector(v)
#======================================================================================================
    \end{arrow format for individual vectors}
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
