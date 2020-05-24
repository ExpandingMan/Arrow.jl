include("utils.jl")
Revise.track("utils.jl")
Revise.track("gendata.jl")
using BenchmarkTools, Debugger

include("pyfbtests.jl")

const FB = Arrow.FB
const Meta = Arrow.Meta
const A = Arrow


function fbroundtrip(obj)
    io = IOBuffer()
    FB.serialize(io, obj)
    seekstart(io)
    FB.deserialize(io, typeof(obj))
end


# TODO for some reason when writing multiple batches, something goes terribly wrong with
# subsequent batches

# TODO now python can't read the batch, claiming something with compression method

df = testdf1(3)[:, 1:1]

pybuf = pyarrowbuffer(df)
jlbuf = Arrow.table(df=>Vector{UInt8})

py_rbbuf = pybuf[(137+8):end]
py_jlbuf = jlbuf[(169+8):end]

pyt = Arrow.Table(pybuf)
jlt = Arrow.Table(jlbuf)

pym = Arrow.readmessage(pybuf, 137+8)
jlm = Arrow.readmessage(jlbuf, 169+8)

pyb = pym.header
jlb = jlm.header
