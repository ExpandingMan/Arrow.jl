
# this file should mirror Schema.fbs

@enum(MetadataVersion::Int16, MetadataVersionV1=0, MetadataVersionV2=1,
      MetadataVersionV3=2, MetadataVersionV4=3)

mutable struct Null end
@ALIGN Null 1

mutable struct Struct end
@ALIGN Struct 1

mutable struct List end
@ALIGN List 1

mutable struct LargeList end
@ALIGN LargeList 1

@with_kw mutable struct FixedSizeList
    listSize::Int32 = 0  # Number of list items per value
end
@ALIGN FixedSizeList 1
FB.slot_offsets(::Type{FixedSizeList}) = UInt32[4]

@with_kw mutable struct Map
    keysSorted::Bool = false  # Set to true if the keys within each value are sorted
end
@ALIGN Map 1
FB.slot_offsets(::Type{Map}) = UInt32[4]

@enum(UnionMode::Int16, UnionModeSparse=0, UnionModeDense=1)

@with_kw mutable struct Union_
    mode::UnionMode = 0
    typeIds::Vector{Int32} = []  # optional, describes typeid of each child
end
@ALIGN Union_ 1
FB.slot_offsets(::Type{Union_}) = UInt32[4, 6]

@with_kw mutable struct Int_
    bitWidth::Int32 = 0  # restricted to 8, 16, 32, and 64 in v1
    is_signed::Bool = false
end
@ALIGN Int_ 1
FB.slot_offsets(::Type{Int_}) = UInt32[4, 6]

@enum(Precision::Int16, PrecisionHALF=0, PrecisionSINGLE=1, PrecisionDOUBLE=2)

@with_kw mutable struct FloatingPoint
    precision::Precision = 0
end
@ALIGN FloatingPoint 1
FB.slot_offsets(::Type{FloatingPoint}) = UInt32[4]

mutable struct Utf8 end
@ALIGN Utf8 1

mutable struct Binary end
@ALIGN Binary 1

mutable struct LargeUtf8 end
@ALIGN LargeUtf8 1

mutable struct LargeBinary end
@ALIGN LargeBinary 1

@with_kw mutable struct FixedSizeBinary
    byteWidth::Int32 = 0  # number of bytes per value
end
@ALIGN FixedSizeBinary 1
FB.slot_offsets(::Type{FixedSizeBinary}) = UInt32[4]

mutable struct Bool_ end
@ALIGN Bool_ 1

@with_kw mutable struct Decimal
    precision::Int32 = 0  # Total number of decimal digits
    scale::Int32 = 0  # Number of digits after the decimal point "."
end
@ALIGN Decimal 1
FB.slot_offsets(::Type{Decimal}) = UInt32[4,6]

# we prepend the `d` to MILLISECOND to distinguish from TimeUnit
@enum(DateUnit::Int16, DateUnitDAY=0, DateUnitMILLISECOND=1)

@with_kw mutable struct Date_
    unit::DateUnit = 1
end
@ALIGN Date_ 1
FB.slot_offsets(::Type{Date_}) = UInt32[4]

@enum TimeUnit::Int16 begin
    TimeUnitSECOND = 0
    TimeUnitMILLISECOND = 1
    TimeUnitMICROSECOND = 2
    TimeUnitNANOSECOND = 3
end

@with_kw mutable struct Time_
    unit::TimeUnit = 1
    bitWidth::Int32 = 32
end
@ALIGN Time_ 1
FB.slot_offsets(::Type{Time_}) = UInt32[4]

@with_kw mutable struct Timestamp
    unit::TimeUnit = 0
    timezone::String = ""
end
@ALIGN Timestamp 1
FB.slot_offsets(::Type{Timestamp}) = UInt32[4,6]

@enum(IntervalUnit::Int16, IntervalUnitYEAR_MONTH=0, IntervalUnitDAY_TIME=1)

@with_kw mutable struct Interval
    unit::IntervalUnit = 0
end
@ALIGN Interval 1
FB.slot_offsets(::Type{Interval}) = UInt32[4]

@with_kw mutable struct Duration
    unit::TimeUnit = 1
end
@ALIGN Duration 1
FB.slot_offsets(::Type{Duration}) = UInt32[4]

@UNION DType (Nothing,
              Null,
              Int_,
              FloatingPoint,
              Binary,
              Utf8,
              Bool_,
              Decimal,
              Date_,
              Time_,
              Timestamp,
              Interval,
              List,
              Struct,
              Union_,
              FixedSizeBinary,
              FixedSizeList,
              Map,
              Duration,
              LargeBinary,
              LargeUtf8,
              LargeList,
             )

@with_kw mutable struct KeyValue
    key::String = ""
    value::String = ""
end
@ALIGN KeyValue 1
FB.slot_offsets(::Type{KeyValue}) = UInt32[4,6]

@enum(DictionaryKind::Int16, DenseArray_=0)

@with_kw mutable struct DictionaryEncoding
    id::Int64 = 0
    indexType::Union{Int_,Nothing} = nothing
    isOrdered::Bool = false
    dictionaryKind::DictionaryKind = 0
end
@ALIGN DictionaryEncoding 1
FB.slot_offsets(::Type{DictionaryEncoding}) = UInt32[4,6,8,10]

@with_kw mutable struct Field
    name::String = ""
    nullable::Bool = false
    dtype_type::UInt8 = 0
    dtype::DType = nothing
    dictionary::Union{DictionaryEncoding,Nothing} = nothing
    children::Vector{Field} = []
    custom_metadata::Vector{KeyValue} = []
end
@ALIGN(Field, 1)
FB.slot_offsets(::Type{Field}) = UInt32[4,6,8,10,12,14,16]

@enum(Endianness::Int16, EndiannessLittle=0, EndiannessBig=1)

@STRUCT struct Buffer
    offset::Int64
    length::Int64
end
@ALIGN(Buffer, 8)

@with_kw mutable struct Schema
    endianness::Endianness = 0
    fields::Vector{Field} = []
    custom_metadata::Vector{KeyValue} = []
end
@ALIGN Schema 1
FB.slot_offsets(::Type{Schema}) = UInt32[4,6,8]
FB.root_type(::Type{Schema}) = true

