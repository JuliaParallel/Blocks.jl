module Blocks

using Base.FS

importall   Base
import      Base.peek, Base.throwto

export      Block, |>, .>, BlockableIO,
            blocks, affinities,
            as_it_is, as_io, as_recordio, as_lines, as_bufferedio, as_bytearray,

            map, mapreduce, pmap, pmapreduce,

            BlockIO, close, eof, read, write, readbytes, peek,
            readall, flush, nb_available, position, filesize, seek, seekend, seekstart, skip

include("pmap.jl")
include("block_io.jl")
include("block_framework.jl")
include("mapreduce.jl")

# sub modules
include("blocked_dataframe.jl")
include("hdfs_blocks.jl")
include("matop_blocks.jl")

end
