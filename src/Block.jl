module Block

using Base.FS
using DataFrames
using HDFS
using HDFS.MapReduce

import Base.pmap

export Blocks

export pmap, pmapreduce

include("blocks.jl")
include("mapreduce.jl")
include("blocked_dataframe.jl")

end
