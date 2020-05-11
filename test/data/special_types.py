import pyarrow as pa

data = [
        pa.array([None, None, None, None, None])
        ]

batch = pa.RecordBatch.from_arrays(data, ["col1"])

sink = pa.BufferOutputStream()

writer = pa.RecordBatchStreamWriter(sink, batch.schema)

for i in range(1):
    writer.write_batch(batch)

writer.close()

buf = sink.getvalue()

b = buf.to_pybytes()  # this is the buffer containing the full streaming format

# schema_buffer = batch.schema.serialize().to_pybytes()

f = open("special_types.dat", "wb")
f.write(b)
f.close()

#import ipdb; ipdb.set_trace()
