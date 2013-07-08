module Block

using Base.FS
using DataFrames
using HDFS
using HDFS.MapReduce

importall   Base
importall   DataFrames
export      Blocks,
            pmap, pmapreduce,
            DDataFrame, dreadtable, 
            colmins, colmaxs, colprods, colsums, colmeans, 
            all, any, isequal,
            nrow, ncol, colnames, colnames!, rename, rename!, index, head, tail, vcat, hcat, rbind, cbind, copy, deepcopy

include("blocks.jl")
include("mapreduce.jl")
include("blocked_dataframe.jl")

end
