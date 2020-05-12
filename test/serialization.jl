using Arrow, BenchmarkTools

using Arrow: write!

# we initialize to 1's because it's easier to tell if something changed
buf = ones(UInt8, 256)

io = IOBuffer(buf, read=true, write=true)
