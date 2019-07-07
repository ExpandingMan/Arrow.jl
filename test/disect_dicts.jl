using Arrow, BenchmarkTools
using Debugger

using Arrow: readmessage, build

const FB = Arrow.FB
const Meta = Arrow.Meta

buf = read("testdata2.dat")

io = IOBuffer(copy(buf))

l1 = reinterpret(Int32, buf[1:4])[1]
m1 = readmessage(buf, 5)
sch = m1.header

idx = 4 + l1 + m1.bodyLength

b1_idx = idx+1
l2 = reinterpret(Int32, buf[(idx+1):(idx+4)])[1]
m2 = readmessage(buf, idx+5)
rb1 = m2.header

idx += 4 + l2

buf2 = buf[(idx+1):end]

idx += m2.bodyLength

b2_idx = idx+1
l3 = reinterpret(Int32, buf[(idx+1):(idx+4)])[1]
m3 = readmessage(buf, idx+5)
rb2 = m3.header

idx += 4 + l3

buf3 = buf[(idx+1):end]

idx += m3.bodyLength

b3_idx = idx+1
l4 = reinterpret(Int32, buf[(idx+1):(idx+4)])[1]
m4 = readmessage(buf, idx+5)
rb3 = m4.header

