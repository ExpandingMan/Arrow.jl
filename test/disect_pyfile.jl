using Arrow, BenchmarkTools

using Arrow: readmessage, build

const FB = Arrow.FB
const Meta = Arrow.Meta

buf = read("testdata1.dat")

io = IOBuffer(copy(buf))

l1 = reinterpret(Int32, buf[1:4]) # 324
m1 = readmessage(buf, 5)
sch = m1.header

l2 = reinterpret(Int32, buf[329:332]) # 348
m2 = readmessage(buf, 333)
rb1 = m2.header

# first buffers start at 680
buf2 = buf[681:end]

l3 = reinterpret(Int32, buf[889:892])
m3 = readmessage(buf, 893)
rb2 = m3.header


p1, node_idx, buf_idx = build(sch.fields[1], rb1, buf2)
p2, node_idx, buf_idx = build(sch.fields[2], rb1, buf2, node_idx, buf_idx)
p3, node_idx, buf_idx = build(sch.fields[3], rb1, buf2, node_idx, buf_idx)
p4, node_idx, buf_idx = build(sch.fields[4], rb1, buf2, node_idx, buf_idx)
