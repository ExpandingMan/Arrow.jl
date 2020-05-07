using Arrow, BenchmarkTools
using Debugger

const FB = Arrow.FB
const Meta = Arrow.Meta

buf = read("testdata1.dat")

ds = Arrow.DataSet(buf)


