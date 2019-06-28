
function Base.write(io::IO, ::Type{Primitive}, v::AbstractVector)
    writepadded(io, reinterpret(UInt8, v))
end

function Base.write(io::IO, ::Type{BitPrimitive}, v::AbstractVector)
    writepadded(io, values(bitpack(v, pad=false)).buffer)
end

function Base.write(io::IO, ::typeof(bitmask), v::AbstractVector)
    writepadded(io, values(bitmask(v, pad=false)).buffer)
end

function Base.write(io::IO, ::typeof(offsets), v::AbstractVector{T}
                   ) where {T<:Union{Vector,String}}
    writepadded(io, offsets(v, pad=false).buffer)
end
