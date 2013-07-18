module Block

using Base.FS
using DataFrames
using HDFS
using HDFS.MapReduce

importall   Base
importall   DataFrames

export      Blocks,
            block, affinity, filter, |>,
            filter_none, filter_file_io, filter_io_recordio, filter_recordio_lines, filter_io_bufferedio,

            pmap, pmapreduce,

            DDataFrame, filter_recordio_dataframe, dreadtable, dwritetable, writetable,
            colmins, colmaxs, colprods, colsums, colmeans, 
            all, any, isequal,
            nrow, ncol, colnames, colnames!, clean_colnames!, rename, rename!, index, 
            head, tail, vcat, hcat, rbind, cbind, copy, deepcopy, isfinite, isnan, isna,
            without, delete!, deleterows!, with, within!, complete_cases, complete_cases!, 
            gather, 
            merge, colwise, by,

            filter_hdfsfile_io
# TODO:
# duplicated, drop_duplicates!
include("blocks.jl")
include("mapreduce.jl")
include("blocked_dataframe.jl")
include("hdfs_blocks.jl")

end
