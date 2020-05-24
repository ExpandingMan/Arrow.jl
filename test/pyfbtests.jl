using PyCall

const pyfb = pyimport("flatbuffers")

module PyAS
using PyCall

const PYARROW_SCHEMA_PATH = "/home/expandingman/src/arrow-schema/python/org/apache/arrow/"
pushfirst!(PyVector(pyimport("sys")."path"), PYARROW_SCHEMA_PATH)

const NAMES = Dict{PyObject,Symbol}()

for (name, str) ∈ [(:Message, "Message"),
                   (:RecordBatch, "RecordBatch"), (:Schema, "Schema"),
                   (:Field, "Field"), (:FieldNode, "FieldNode")]
    @eval const $name = pyimport("flatbuf."*$str)
    @eval NAMES[$name] = Symbol($str)
end

function frombuf(obj::PyObject, buf::AbstractVector{UInt8}, o::Integer=1)
    name = NAMES[obj]
    s = Symbol(string("GetRootAs",name))
    getproperty(getproperty(obj, NAMES[obj]), s)(buf, o-1)
end
function gettablefield(obj::PyObject, ftype::PyObject, n::Symbol)
    ϕ = ftype()
    ϕ.Init(getproperty(obj, n).Bytes, getproperty(obj, n).Pos)
end

end
