#= test/gendata.jl
Here we have useful functions for generating data from the C++ arrow implementation via pyarrow
and PyCall.

This of course does not use Arrow.jl in any way whatsoever, but it uses DataFrames for convenience.

## Python Dependencies
- `pyarrow`
- `numpy`
- `pandas`
=====================================================================================================#
using PyCall, Tables, DataFrames, PooledArrays
using Random, Dates

Random.seed!(999)

const pa = pyimport("pyarrow")
const np = pyimport("numpy")
const pd = pyimport("pandas")

const pynone = pybuiltin("None")

# NOTE: write IPC streams with RecordBatchStreamWriter and files with RecordBatchFileWriter

_make_py_happy(v::PooledVector) = pd.Categorical([_make_py_happy(x) for x ∈ v])
_make_py_happy(v::AbstractVector) = [_make_py_happy(x) for x ∈ v]
_make_py_happy(::Missing) = pynone
_make_py_happy(x) = x
pa_array(v::AbstractVector) = pa.array(_make_py_happy(v))

"""
    pybatch(df)

Return a `pyarrow` batch object from table (with Tables.jl interface) `df`.
"""
function pybatch(df)
    pa.RecordBatch.from_arrays([pa_array(c) for c ∈ Tables.columntable(df)],
                               string.(Tables.columnnames(df)))
end

"""
    pyarrowbuffer(b::PyObject; nbatches::Integer=1, writer=pa.RecordBatchStreamWriter)
    pyarrowbuffer(df; nbatches::Integer=1, writer=pa.RecordBatchStreamWriter)

Create a buffer (`Vector{UInt8}`) from a pyarrow batch `b` or table `df` (which will automatically
be converted to a batch).
"""
function pyarrowbuffer(b::PyObject; nbatches::Integer=1,
                       writer::PyObject=pa.RecordBatchStreamWriter)
    snk = pa.BufferOutputStream()
    w = writer(snk, b.schema)
    for i ∈ 1:nbatches
        w.write_batch(b)
    end
    w.close()
    # we instantiate this mostly just for the hell of it
    Vector(reinterpret(UInt8, snk.getvalue()))
end
pyarrowbuffer(df; kwargs...) = pyarrowbuffer(pybatch(df); kwargs...)

"""
    writepyfile(fname, bordf; kwargs...)

Write a file to filename `fname` from a pyarrow batch object or table `bordf`.

Accepts same keyword arguments as `pyarrowbuffer`.
"""
writepyfile(fname::AbstractString, bordf; kwargs...) = write(fname, pyarrowbuffer(bordf; kwargs...))


"""
    testdf1(N)

A very basic test table with only (non-null) bits-types and strings.
"""
function testdf1(N::Integer=10)
    DataFrame(col1=rand(0:9, N), col2=rand(N), col3=[randstring(rand(0:12)) for n ∈ 1:N])
end

"""
    testdf2()

The "original" test dataframe with all of the basic nesting and nullable cases, but no weird types.
"""
function testdf2()
    DataFrame(col1=[1,2,3,4],
              col2=[1.0, missing, 3.0, missing],
              col3=["fire", "walk", "with", "me"],
              col4=[[1,2], [3,4], [5,6], [7,8,9]],
              col5=[missing, "kirk", "αβabcdefg", "spock"],
              # evenutally we want to support decoding this in Arrow, but for now need annotations
              col6=[[1.0, missing, 2.0], Union{Float64,Missing}[2.0, 3.0, 4.0],
                    Union{Float64,Missing}[missing, missing], missing],
              col7=[["ab", "αβ"], ["kirk", "spock", "bones"], ["123"], ["fire"]],
              col8=[["abc"], missing, ["123", "fire"], ["walk", "with"]],
              # again, we need this for now
              col9=[["abcd", missing], Union{String,Missing}["kirk", "spock"], missing,
                    Union{String,Missing}["bones"]]
             )
end

"""
    testdf3(N)

Test some weirder types.
"""
function testdf3(N::Integer=10)
    DataFrame(col1=fill(missing, N),
             )
end

"""
    testdf4()

Test some very deeply nested stuff.
"""
function testdf4()
    DataFrame(col1=[ [[1,2], [3,4,5], [6]], [[8,9]] ],
              col2=[ [missing, [1,2], [3,4]], missing],
             )
end

"""
    testdf5()

Some test of dictionary encoded vectors.
"""
function testdf5()
    DataFrame(col1=PooledArray([4.0, 5.0, 5.0, 6.0, 7.0, 6.0, 4.0]),
              col2=[4, 5, 5, 6, 7, 6, 4],
              col3=PooledArray([2, missing, 2, 3, missing, 1, missing]),
             )
end
