module Blocks

using Base.FS
using Compat

importall   Base
import      Base: peek, throwto, AsyncStream

if isless(Base.VERSION, v"0.4.0-")
import      Base.localpart
else
import      Base.|>
end

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
