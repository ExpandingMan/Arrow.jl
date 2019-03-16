
# this file should mirror Schema.fbs

@enum(MetadataVersion::Int16, V1, V2, V3, V4)

# NOTE: we use Julia's `Nothing` as the `Null` type

struct Struct end

struct List end

@with_kw mutable struct FixedSizeList
    listSize::Int32  # Number of list items per value
end

@with_kw mutable struct Map
    keysSorted::Bool  # Set to true if the keys within each value are sorted
end

@enum(UnionMode::Int16, Sparse, Dense)

@with_kw mutable struct Union_
    mode::UnionMode
    typeIds::Vector{Int32}  # optional, describes typeid of each child
end

@with_kw mutable struct Int_
    bitWidth::Int32  # restricted to 8, 16, 32, and 64 in v1
    is_signed::Bool
end

@enum(Precision::Int16, HALF, SINGLE, DOUBLE)

@with_kw mutable struct FloatingPoint
    precision::Precision
end

struct Utf8 end

struct Binary end

@with_kw mutable struct FixedSizeBinary
    byteWidth::Int32  # number of bytes per value
end

struct Bool_ end

@with_kw mutable struct Decimal
    precision::Int32  # Total number of decimal digits
    scale::Int32  # Number of digits after the decimal point "."
end

# we prepend the `d` to MILLISECOND to distinguish from TimeUnit
@enum(DateUnit::Int16, DAY, dMILLISECOND)

@with_kw mutable struct Date_
    unit::DateUnit = dMILLISECOND
end

@enum(TimeUnit::Int16, SECOND, MILLISECOND, MICROSECOND, NANOSECOND)

@with_kw mutable struct Time_
    unit::TimeUnit = MILLISECOND
    bitWidth::Int32 = 32
end

@with_kw mutable struct Timestamp
    unit::TimeUnit
    timezone::String
end

@enum(IntervalUnit::Int16, YEAR_MONTH, DAY_TIME)

@with_kw mutable struct Interval
    unit::IntervalUnit
end

@UNION DType (Nothing,Int_,FloatingPoint,Binary,Utf8,Bool_,Decimal,Date_,Time_,
              Timestamp,Interval,List,Struct,Union_,FixedSizeBinary,FixedSizeList,
              Map)

@with_kw mutable struct KeyValue
    key::String
    value::String
end

@with_kw mutable struct DictionaryEncoding
    id::Int64
    indexType::Int_
    isOrdered::Bool_
end

@with_kw mutable struct Field
    name::String
    nullable::Bool
    dtype::DType
    dictionary::DictionaryEncoding
    children::Vector{Field}
    custom_metadata::Vector{KeyValue}
end

@enum(Endianness::Int16, Little, Big)

@with_kw mutable struct Buffer
    offset::Int64
    length::Int64
end

@with_kw mutable struct Schema
    endianness::Endianness = Little
    fields::Vector{Field}
    custom_metadata::Vector{KeyValue}
end
