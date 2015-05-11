##
# file sorting demo
# julia -p 5
# > require("filesort.jl")
# > filesort("test.csv", 20)


using Blocks
using Compat
using Base.FS

tmpf(workfile::AbstractString) = string(myid())*"_"*string(@compat(Int(time_ns())))*"_"*workfile

function merge_blocks(workfile::AbstractString, inio::Array) 
    (length(inio) == 1) && return inio[1]
    out = tmpf(workfile)
    merge_streams(open(out, "w"), map(x->open(x), inio)...)
    map(x->rm(x), inio)
    out
end
function merge_streams(out::IO, _inio::IO...)
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

function sort_step_2(workfile::AbstractString, block_files::Array, nway::Int=0)
    # make pairs of files for pmap
    (0 == nway) && (nway = length(block_files))
    npairs = @compat(Int(ceil(length(block_files)/nway)))
    mb = cell(npairs)
    for idx in 1:npairs
        st = (idx-1)*nway+1
        ed = min(length(block_files), st+nway-1)
        mb[idx] = block_files[st:ed]
    end
    block_files = pmap(x->merge_blocks(workfile, x), mb) 
    println("\tmerge -> $(length(block_files))...")
    #for fname in block_files
    #    println("\t$(fname)")
    #end
    block_files
end

function sort_step_1(workfile::AbstractString, n::Int)
    function as_tempfile(lines)
        fname = tmpf(workfile)
        open(fname, "w") do io
            for c in lines
                write(io, c)
            end
        end
        fname
    end
    b = Block(File(workfile), n) |> as_io |> as_recordio |> as_lines |> sort |> as_tempfile
    block_files = pmap(x->x, b)
    println("\tblocks sorted -> $(length(block_files))...")
    for fname in block_files
        println("\t$(fname)")
    end
    block_files
end

function filesort(workfile::AbstractString, n::Int)
    block_files = sort_step_1(workfile, n)
    while(length(block_files) > 1)
        block_files = sort_step_2(workfile, block_files)
    end
    block_files[1]
end

if length(ARGS) > 0
    println(ARGS)
end

