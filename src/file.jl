
file(fname::AbstractString; use_mmap::Bool=true) = file(use_mmap ? Mmap.mmap(fname) : read(fname))

function _validate_magic_bytes(b::Vector{UInt8})
    b[1:6] == FILE_FORMAT_MAGIC_BYTES ||
        throw(ArgumentError("Invalid arrow file: magic bytes $(String(b[1:min(8,length(b))]))"*
                            ", expected \"$FILE_FORMAT_MAGIC_BYTES\""))
end

file(buf::Vector{UInt8}) = (_validate_magic_bytes(buf); Table(buf, ALIGNMENT+1))
file(io::IO) = (_validate_magic_bytes(read(io, ALIGNMENT)); Table(io))

# TODO need to deal with footer and do writing
