import pyarrow as pa

data = [
        pa.array([1,2,3,4]),
        pa.array([1.0, None, 2.0, None]),
        pa.array(["fire", "walk", "with", "me"]),
        ]

batch = pa.RecordBatch.from_arrays(data, ["a", "b", "c"])

sink = pa.BufferOutputStream()

writer = pa.RecordBatchStreamWriter(sink, batch.schema)

writer.write_batch(batch)

writer.close()

buf = sink.getvalue()

b = buf.to_pybytes()  # this is the buffer containing the full streaming format

# schema_buffer = batch.schema.serialize().to_pybytes()

f = open("minimal_stream.dat", "wb")
f.write(b)
f.close()

#import ipdb; ipdb.set_trace()
