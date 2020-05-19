using Arrow, BenchmarkTools, DataFrames, Tables
using Debugger

const FB = Arrow.FB
const Meta = Arrow.Meta
const A = Arrow

# NOTE: currently need `copycols=false` for lazily loaded dataframe

using Arrow: build

#=
buf = read("data/basic_stream.dat")

t = Arrow.Table(buf)
df = DataFrame(t, copycols=false)

# currently broken starting at column 6
df1 = DataFrame(col6=df.col6)

vs = collect(eachcol(df1))

io = IOBuffer()
t1 = Arrow.Table!(io, Tables.schema(df1), vs)

t2 = Arrow.Table((seekstart(io); read(io)))

b = Arrow.batches(t)[1]
b1 = Arrow.batches(t1)[1]
b2 = Arrow.batches(t2)[1]
=#
# TODO this needs the child nodes to be able to work!!
