import pyarrow as pa
import pandas as pd

v = pa.array([1,2,3,4])

data = [
        pa.array(pd.Categorical([4.0, 5.0, 5.0, 6.0, 7.0, 6.0, 4.0])),
        pa.array([4, 5, 5, 6, 7, 6, 4]),
        pa.array(pd.Categorical([2, None, 2, 3, None, 1, None]))
        ]

batch = pa.RecordBatch.from_arrays(data, ["col1", "col2", "col3"])

sink = pa.BufferOutputStream()

writer = pa.RecordBatchStreamWriter(sink, batch.schema)

for i in range(2):
    writer.write_batch(batch)

writer.close()

buf = sink.getvalue()

b = buf.to_pybytes()  # this is the buffer containing the full streaming format

# schema_buffer = batch.schema.serialize().to_pybytes()

f = open("testdata2.dat", "wb")
f.write(b)
f.close()

#import ipdb; ipdb.set_trace()
