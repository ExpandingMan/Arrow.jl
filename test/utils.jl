#= test/utils.jl
Here we have some utilities for easily testing data.  These functions can be used together with
`gendata.jl` to rapidly test.

Note that it's *NOT* intended for all unit tests to be implemented with these functions, but
it can be extremely useful for rapid troubleshooting.
======================================================================================================#
include("gendata.jl")
using Arrow

pyreadbuffer(v::AbstractVector{UInt8}) = pa.ipc.open_stream(v).read_pandas()

# TODO this doesn't know if it's getting the file format
tablefrompy(df; kwargs...) = Arrow.Table(pyarrowbuffer(df; kwargs...))

function tablefromjl(df; nbatches::Integer=1)
    io = IOBuffer()
    Arrow.Table!(io, df, nbatches=nbatches)
    seekstart(io)
    Arrow.Table(read(io))
end
