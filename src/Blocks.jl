module Blocks

using Base.FS

importall   Base
import      Base.peek, Base.throwto, Base.AsyncStream, Base.localpart

export      Block, |>, .>, prepare, @prepare, BlockableIO,
            blocks, affinities, localpart,
            as_it_is, as_io, as_recordio, as_lines, as_bufferedio, as_bytearray,

            map, mapreduce, pmap, pmapreduce,

            BlockIO, close, eof, read, write, readbytes, peek,
            readall, flush, nb_available, position, filesize, seek, seekend, seekstart, skip

include("pmap.jl")
include("block_io.jl")
include("block_framework.jl")
include("mapreduce.jl")

# sub modules
#include("dataframe_blocks.jl")
#include("hdfs_blocks.jl")
include("matop_blocks.jl")

end
