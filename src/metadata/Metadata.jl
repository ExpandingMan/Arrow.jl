module Metadata

using Dates
using FlatBuffers; const FB = FlatBuffers

include("schema.jl")
include("tensor.jl")
include("sparsetensor.jl")
include("message.jl")
include("file.jl")

# TODO should probably change all names to a specialized namespace with a prefix

#=======================================================================================================
    \begin{additional constructors}
=======================================================================================================#
# NOTE: no idea how I'll do dictionary stuff with this yet
function childfield(::Type{T};
                    dictionary::Union{DictionaryEncoding,Nothing}=nothing,
                    custom_metadata=Dict(), name::Union{AbstractString,Symbol}="item") where {T}
    t = childtype(T)
    isnothing(t) && return nothing
    Field(string(name), t, dictionary=dictionary, custom_metadata=custom_metadata)
end

function RecordBatch(l::Integer,
                     nodes::AbstractVector{FieldNode}=Vector{FieldNode}(undef,0),
                     bufs::AbstractVector{Buffer}=Vector{Buffer}(undef,0))
    Meta.RecordBatch(l, nodes, bufs, nothing)
end

# TODO will need more arguments for more complicated types
function Field(name::Union{AbstractString,Symbol}, ::Type{T};
               dictionary::Union{DictionaryEncoding,Nothing}=nothing,
               custom_metadata=Dict()) where {T}
    child = childfield(T)
    Field(string(name), isnullabletype(T), FB.typeorder(DType, typeof(arrowtype(T))),
          arrowtype(T), dictionary,
          child == nothing ? [] : [childfield(T)],
          [KeyValue(kv) for kv ∈ pairs(custom_metadata)])
end

KeyValue(p::Pair{<:Union{AbstractString,Symbol},<:Union{AbstractString,Symbol}}) = KeyValue(string(p[1]),
                                                                                            string(p[2]))
#=======================================================================================================
    \end{additional constructors}
=======================================================================================================#


struct InvalidMetadataError <: Exception
    msg::String
end

function juliatype(int::Int_)
    if int.is_signed
        if int.bitWidth == 8
            Int8
        elseif int.bitWidth == 16
            Int16
        elseif int.bitWidth == 32
            Int32
        elseif int.bitWidth == 64
            Int64
        elseif int.bitWidth == 128
            Int128
        else
            throw(InvalidMetadataError("$int is not valid arrow type metadata"))
        end
    else
        if int.bitWidth == 8
            UInt8
        elseif int.bitWidth == 16
            UInt16
        elseif int.bitWidth == 32
            UInt32
        elseif int.bitWidth == 64
            UInt64
        elseif int.bitWidth == 128
            UInt128
        else
            throw(InvalidMetadataError("$int is not valid arrow type metadata"))
        end
    end
end
function juliatype(fp::FloatingPoint)
    if fp.precision == PrecisionHALF
        Float16
    elseif fp.precision == PrecisionSINGLE
        Float32
    elseif fp.precision == PrecisionDOUBLE
        Float64
    else
        throw(InvalidMetadataError("$fp is not valid arrow type metadata"))
    end
end
juliatype(::Null) = Missing

isnullabletype(::Type{T}) where {T} = T <: Union{S,Missing} where {S}

arrowtype(::Type{Int8}) = Int_(8, true)
arrowtype(::Type{Int16}) = Int_(16, true)
arrowtype(::Type{Int32}) = Int_(32, true)
arrowtype(::Type{Int64}) = Int_(64, true)
arrowtype(::Type{Int128}) = Int_(128, true)
arrowtype(::Type{UInt8}) = Int_(8, false)
arrowtype(::Type{UInt16}) = Int_(16, false)
arrowtype(::Type{UInt32}) = Int_(32, false)
arrowtype(::Type{UInt64}) = Int_(64, false)
arrowtype(::Type{UInt128}) = Int_(128, false)

arrowtype(::Type{Bool}) = Bool_()

arrowtype(::Type{Float16}) = FloatingPoint(PrecisionHALF)
arrowtype(::Type{Float32}) = FloatingPoint(PrecisionSINGLE)
arrowtype(::Type{Float64}) = FloatingPoint(PrecisionDOUBLE)

arrowtype(::Type{<:AbstractString}) = Utf8()

arrowtype(::Type{Date}) = Date_(DateUnitDAY)
arrowtype(::Type{Time}) = Time_(TimeUnitNANOSECOND)
arrowtype(::Type{DateTime}) = Timestamp(TimeUnitMILLISECOND, "")

arrowtype(::Type{<:AbstractVector}) = List()

arrowtype(::Type{Union{T,Missing}}) where {T} = arrowtype(T)

childtype(::Type) = nothing
childtype(::Type{<:AbstractVector{T}}) where {T} = T
childtype(::Type{<:Union{Missing,<:AbstractVector{T}}}) where {T} = T

function readmessage(buf::AbstractVector{UInt8}, i::Integer, j::Integer=length(buf))
    FB.read(Message, @view buf[i:j])
end
readmessage(io::IO, ℓ::Integer) = FB.read(Message, read(io, ℓ))
readmessage(io::IO) = readmessage(io, read(io, Int32))


export juliatype, arrowtype, readmessage
export FB

end # module
