using Arrow, BenchmarkTools
using Debugger

using Arrow: readmessage, build

const FB = Arrow.FB
const Meta = Arrow.Meta

buf = read("testdata1.arrow")

io = IOBuffer(copy(buf))

idx = 8  # file starts with b"ARROW1\0\0"

l1 = reinterpret(Int32, buf[(idx+1):(idx+4)])[1]
m1 = readmessage(buf, idx+5)
sch = m1.header

# idx of start of next
idx = 8 + 4 + l1 + m1.bodyLength

b1_idx = idx+1
l2 = reinterpret(Int32, buf[(idx+1):(idx+4)])[1]
m2 = readmessage(buf, idx+5)
rb1 = m2.header

idx += 4 + l2

# first data buffer, read arrays from this
buf2 = buf[(idx+1):end]

idx += m2.bodyLength

b2_idx = idx+1
l3 = reinterpret(Int32, buf[(idx+1):(idx+4)])[1]
m3 = readmessage(buf, idx+5)
rb2 = m3.header


println("building integer array...")
p1, node_idx, buf_idx = build(sch.fields[1], rb1, buf2)

println("building nullable float array, $node_idx, $buf_idx...")
p2, node_idx, buf_idx = build(sch.fields[2], rb1, buf2, node_idx, buf_idx)

println("building string array, $node_idx, $buf_idx...")
p3, node_idx, buf_idx = build(sch.fields[3], rb1, buf2, node_idx, buf_idx)

println("building nested list, $node_idx, $buf_idx...")
p4, node_idx, buf_idx = build(sch.fields[4], rb1, buf2, node_idx, buf_idx)

println("building nullable string array, $node_idx, $buf_idx...")
p5, node_idx, buf_idx = build(sch.fields[5], rb1, buf2, node_idx, buf_idx)

#node_idx, buf_idx = 7, 15
println("building nullable nested list, $node_idx, $buf_idx...")
p6, node_idx, buf_idx = build(sch.fields[6], rb1, buf2, node_idx, buf_idx)

println("building nested list with strings, $node_idx, $buf_idx...")
p7, node_idx, buf_idx = build(sch.fields[7], rb1, buf2, node_idx, buf_idx)

println("building nested list with outer nullables, $node_idx, $buf_idx...")
p8, node_idx, buf_idx = build(sch.fields[8], rb1, buf2, node_idx, buf_idx)

println("building nested list with inner and outer nullables, $node_idx, $buf_idx...")
p9, node_idx, buf_idx = build(sch.fields[9], rb1, buf2, node_idx, buf_idx)
