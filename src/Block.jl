module Block

using Base.FS
using DataFrames
using HDFS
using HDFS.MapReduce

import Base.pmap

export Blocks

export pmap, pmapreduce

include("mapreduce.jl")

end
