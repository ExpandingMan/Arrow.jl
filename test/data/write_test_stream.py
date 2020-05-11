import pyarrow as pa

data = [
        pa.array([1,2,3,4]),
        pa.array([1.0, None, 2.0, None]),
        pa.array(["fire", "walk", "with", "me"]),
        pa.array([[1,2], [3,4], [5,6], [7,8,9]]),
        pa.array([None, "kirk", "αβabcdefg", "spock"]),
        pa.array([[1.0, None, 2.0], [2.0, 3.0, 4.0], [None, None], None]),
        pa.array([["ab", "αβ"], ["kirk", "spock", "bones"], ["123"], ["fire"]]),
        pa.array([["abc"], None, ["123", "fire"], ["walk", "with"]]),
        pa.array([["abcd", None], ["kirk", "spock"], None, ["bones"]])
        ]

batch = pa.RecordBatch.from_arrays(data, ["col1", "col2", "col3", "col4", "col5", "col6", "col7",
                                          "col8", "col9"])

sink = pa.BufferOutputStream()

writer = pa.RecordBatchStreamWriter(sink, batch.schema)

for i in range(2):
    writer.write_batch(batch)

writer.close()

buf = sink.getvalue()

b = buf.to_pybytes()  # this is the buffer containing the full streaming format

# schema_buffer = batch.schema.serialize().to_pybytes()

f = open("testdata1.dat", "wb")
f.write(b)
f.close()

#import ipdb; ipdb.set_trace()
