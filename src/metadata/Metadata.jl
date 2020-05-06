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
function Field(name::AbstractString, nullable::Bool, dtype::DType,
               dictionary::Union{DictionaryEncoding,Nothing}=nothing, children::AbstractVector=[],
               custom_metadata::AbstractVector=[])
    Field(name, nullable, FB.typeorder(DType, typeof(dtype)), dtype, dictionary, children,
          custom_metadata)
end
#=======================================================================================================
    \end{additional constructors}
=======================================================================================================#


# TODO have this give decent error messages
struct InvalidMetadataError <: Exception end

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
            throw(InvalidMetadataError())
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
            throw(InvalidMetadataError())
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
        throw(InvalidMetadataError())
    end
end


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


function readmessage(buf::AbstractVector{UInt8}, i::Integer, j::Integer=length(buf))
    FB.read(Message, @view buf[i:j])
end
readmessage(io::IO, ℓ::Integer) = FB.read(Message, read(io, ℓ))
readmessage(io::IO) = readmessage(io, read(io, Int32))


export juliatype, arrowtype, readmessage
export FB

end # module
