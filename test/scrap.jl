using Arrow, BenchmarkTools
using Debugger

const FB = Arrow.FB
const Meta = Arrow.Meta
const A = Arrow

buf = read("testdata1.dat")

ds = Arrow.DataSet(buf)


