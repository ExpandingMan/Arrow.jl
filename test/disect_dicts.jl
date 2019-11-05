using Arrow, BenchmarkTools
using Debugger

using Arrow: readmessage, build

const FB = Arrow.FB
const Meta = Arrow.Meta

# note that these now start with continuation indicators of 0xffffffff

buf = read("testdata2.dat")

io = IOBuffer(copy(buf))

# skip continuation indicator
l1 = reinterpret(Int32, buf[5:8])[1]
m1 = readmessage(buf, 9)
sch = m1.header

idx = 8 + l1 + m1.bodyLength

b1_idx = idx+1
l2 = reinterpret(Int32, buf[(idx+5):(idx+8)])[1]
m2 = readmessage(buf, idx+9)
rb1 = m2.header

idx += 8 + l2

buf2 = buf[(idx+1):end]

idx += m2.bodyLength

b2_idx = idx+1
l3 = reinterpret(Int32, buf[(idx+5):(idx+8)])[1]
m3 = readmessage(buf, idx+9)
rb2 = m3.header

idx += 8 + l3

buf3 = buf[(idx+1):end]

idx += m3.bodyLength

b3_idx = idx+1
l4 = reinterpret(Int32, buf[(idx+5):(idx+8)])[1]
m4 = readmessage(buf, idx+9)
rb3 = m4.header

