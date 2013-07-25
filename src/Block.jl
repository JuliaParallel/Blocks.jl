module Block

using Base.FS
using DataFrames
using HDFS
using HDFS.MapReduce

importall   Base
importall   DataFrames

export      Blocks, |>,
            blocks, affinities,
            as_it_is, as_io, as_recordio, as_lines, as_bufferedio, as_bytearray,

            map, mapreduce, pmap, pmapreduce,

            DDataFrame, as_dataframe, dreadtable, dwritetable, writetable,
            colmins, colmaxs, colprods, colsums, colmeans, 
            all, any, isequal,
            nrow, ncol, colnames, colnames!, clean_colnames!, rename, rename!, index, 
            head, tail, vcat, hcat, rbind, cbind, copy, deepcopy, isfinite, isnan, isna,
            without, delete!, deleterows!, with, within!, complete_cases, complete_cases!, 
            gather, 
            merge, colwise, by

# TODO:
# duplicated, drop_duplicates!
include("pmap.jl")
include("blocks.jl")
include("mapreduce.jl")
include("blocked_dataframe.jl")
include("hdfs_blocks.jl")

end
