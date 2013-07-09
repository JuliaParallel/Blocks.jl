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
            nrow, ncol, colnames, colnames!, clean_colnames!, rename, rename!, index, 
            head, tail, vcat, hcat, rbind, cbind, copy, deepcopy, isfinite, isnan,
            without, delete!, with, within!,
            gather, 
            merge
# TODO:
# complete_cases, complete_cases!, duplicated, drop_duplicates!
include("blocks.jl")
include("mapreduce.jl")
include("blocked_dataframe.jl")

end
