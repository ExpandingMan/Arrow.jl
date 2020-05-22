include("utils.jl")
Revise.track("utils.jl")
Revise.track("gendata.jl")
using BenchmarkTools, Debugger

const FB = Arrow.FB
const Meta = Arrow.Meta
const A = Arrow


# TODO for some reason when writing multiple batches, something goes terribly wrong with
# subsequent batches


# TODO also, the below is to try to sort out some disaster that's happening with
# python not being able to read the flatbuffers
df = testdf1(3)[:, 1:1]

pybuf = pyarrowbuffer(df)
jlbuf = Arrow.table(df=>Vector{UInt8})

pystr = String(copy(pybuf))
jlstr = String(copy(jlbuf))

pyt = Arrow.Table(pybuf)
jlt = Arrow.Table(jlbuf)

pysch = pyt.schema.header
jlsch = jlt.schema.header

pyϕ = pysch.fields[1]
jlϕ = jlsch.fields[1]
