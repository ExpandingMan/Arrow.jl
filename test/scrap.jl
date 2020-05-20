using Arrow, BenchmarkTools, DataFrames, Tables
using Debugger

const FB = Arrow.FB
const Meta = Arrow.Meta
const A = Arrow

# NOTE: currently need `copycols=false` for lazily loaded dataframe

using Arrow: build

buf = read("data/deep_nesting.dat")

t = Arrow.Table(buf)
df = DataFrame(t, copycols=false)

v = df.col2
vv = view(v, 1:2)

w = A.values(v)
ww = A.values(vv)
w1 = A.values(w)
ww1 = A.values(ww)

# currently broken starting at column 6
#=
df1 = DataFrame(col6=df.col6)

vs = collect(eachcol(df1))

io = IOBuffer()
t1 = Arrow.Table!(io, Tables.schema(df1), vs)

t2 = Arrow.Table((seekstart(io); read(io)))

b = Arrow.batches(t)[1]
b1 = Arrow.batches(t1)[1]
b2 = Arrow.batches(t2)[1]
=#
