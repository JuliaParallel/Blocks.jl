##
# file sorting demo
# julia -p 5
# > require("filesort.jl")
# > filesort("test.csv", 20)


using Block
using Base.FS

tmpf(workfile::String) = string(myid())*"_"*string(int(time_ns()))*"_"*workfile

function sort_blocks(workfile::String, inio::Array) 
    (length(inio) == 1) && return inio[1]
    out = tmpf(workfile)
    sort_streams(open(out, "w"), map(x->open(x), inio)...)
    map(x->rm(x), inio)
    out
end
function sort_streams(out::IO, _inio::IO...)
    inio = [_inio...]
    nio = length(inio)
    lines = map(x->readline(x), inio)
    while(length(inio) > 0)
        (str,pos) = findmin(lines)
        print(out, str)
        if eof(inio[pos])
            close(splice!(inio, pos))
            splice!(lines, pos)
        else
            lines[pos] = readline(inio[pos])
        end
    end
    close(out)
    return
end

function sort_step_2(workfile::String, block_files::Array)
    # make pairs of files for pmap
    npairs = int(ceil(length(block_files)/2))
    mb = cell(npairs)
    for idx in 1:npairs
        st = (idx-1)*2+1
        ed = min(length(block_files), st+1)
        mb[idx] = block_files[st:ed]
    end
    block_files = pmap(x->sort_blocks(workfile, x), mb) 
    println("\tmerge -> $(length(block_files))...")
    #for fname in block_files
    #    println("\t$(fname)")
    #end
    block_files
end

function sort_step_1(workfile::String, n::Int)
    b = Blocks(File(workfile), Array, n)
    block_files = pmap(c->begin 
        fname = tmpf(workfile)
        io = open(fname, "w")
        write(io, sort(c))
        close(io)
        fname
        end, b)
    println("\tblocks sorted -> $(length(block_files))...")
    #for fname in block_files
    #    println("\t$(fname)")
    #end
    block_files
end

function filesort(workfile::String, n::Int)
    block_files = sort_step_1(workfile, n)
    while(length(block_files) > 1)
        block_files = sort_step_2(workfile, block_files)
    end
    block_files[1]
end


