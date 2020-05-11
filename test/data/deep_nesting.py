import pyarrow as pa

data = [
        pa.array([ [[1,2], [3,4,5], [6]], [[8, 9]] ]),
        pa.array([ [None, [1,2], [3,4]], None ])
        ]

batch = pa.RecordBatch.from_arrays(data, ["col1", "col2"])

sink = pa.BufferOutputStream()

writer = pa.RecordBatchStreamWriter(sink, batch.schema)

for i in range(1):
    writer.write_batch(batch)

writer.close()

buf = sink.getvalue()

b = buf.to_pybytes()  # this is the buffer containing the full streaming format

# schema_buffer = batch.schema.serialize().to_pybytes()

f = open("deepnest.dat", "wb")
f.write(b)
f.close()

#import ipdb; ipdb.set_trace()
