using Arrow, BenchmarkTools

using Arrow: readmessage, build

const FB = Arrow.FB
const Meta = Arrow.Meta

buf = read("testdata1.dat")

io = IOBuffer(copy(buf))

l1 = reinterpret(Int32, buf[1:4]) # 460, length only of this message
m1 = readmessage(buf, 5)
sch = m1.header

l2 = reinterpret(Int32, buf[465:468]) # 508, length only of this message
m2 = readmessage(buf, 469)
rb1 = m2.header

# first buffers start at 680
buf2 = buf[977:end]

l3 = reinterpret(Int32, buf[1345:1348])
m3 = readmessage(buf, 1349)
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
