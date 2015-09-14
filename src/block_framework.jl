typealias BlockableIO Union{IOStream,LibuvStream,IOBuffer,BlockIO}

type Block{T}
    source::T
    block::Array            # chunk definition
    affinity::Array         # chunk affinity
    filter::Function        # returns a processed block. can be chained. default just passes the chunk definition from "block"
    prepare::Function
end

const no_affinity = []
const loopbackip = IPv4(127,0,0,1)

##
# Pipelined filters
# TODO: relook at IO integration after IO enhancements in base
as_it_is(x) = x

as_io(x) = open(x)
as_io(f::File) = open(f.path)
function as_io(x::Tuple)
    if isa(x[1], LibuvStream)
        aio = x[1]
        maxsize = x[2]
        start_reading(aio)
        Base.wait_readnb(aio, maxsize)
        avlb = min(nb_available(aio.buffer), maxsize)
        return PipeBuffer(read(aio, Array(UInt8,avlb)))
    elseif isa(x[1], BlockableIO)
        io = x[1]
        # using BlockIO to avoid copy.
        # but since BlockIO would skip till the first record delimiter,
        # we must present to it the last read byte (which must be the last read delimiter)
        # therefore we pass the last read position as the start position for BlockIO
        pos = position(io)
        endpos = pos+x[2]
        (0 == pos) && (pos = 1)
        return BlockIO(io, pos:endpos)
    else
        return BlockIO(open(x[1]), x[2])
    end
end
function as_bufferedio(x::BlockableIO)
    buff = read(x, Array(UInt8, nb_available(x)))
    close(x)
    IOBuffer(buff)
end
as_lines(x::BlockableIO) = readlines(x)
as_bytearray(x::BlockableIO) = read(x, Array(UInt8, nb_available(x)))

as_recordio(x::BlockIO, dlm::Char='\n') = BlockIO(x, dlm)
function as_recordio(x::Tuple)
    dlm = (length(x) == 3) ? x[3] : '\n'
    if isa(x[1], LibuvStream)
        aio = x[1]
        minsize = x[2]
        start_reading(aio)
        Base.wait_readnb(aio, minsize)
        tot_avlb = nb_available(aio.buffer)
        avlb = min(tot_avlb, minsize)
        pb = PipeBuffer(read(aio, Array(UInt8,avlb)))
        write(pb, readuntil(aio, dlm))
        return pb
    else
        return BlockIO(as_io(x), dlm)
    end
end

as_wordio(x::BlockIO, dlm::Char=' ') = BlockIO(x, dlm)

|>(b::Block, f::Function) = filter(f, b)
function filter(f::Function, b::Block)
    let oldf = b.filter
        b.filter = (x)->f(oldf(x))
        return b
    end
end

function prepare(b::Block, f::Function)
    let oldf = b.prepare
        b.prepare = (x)->f(oldf(x))
        return b
    end
end

macro prepare(expr)
    local nexpr = expr
    while(isa(nexpr, Expr))
        (nexpr.args[1] == :|>) && (nexpr.args[1] = :prepare)
        nexpr = nexpr.args[2]
    end
    esc(expr)
end

##
# Iterators for blocks and affinities
# There are separate iterators for data and its affinity, because affinity is sometimes common to all chunks.
# But both can be iterated upon together by zip(blocks(b), affinities(b)).
abstract BlockIterator{T}

type BlockAffinityIterator{T} <: BlockIterator{T}
    b::Block{T}
end

type BlockDataIterator{T} <: BlockIterator{T}
    b::Block{T}
end

start{T<:BlockIterator}(bi::T) = 1
done{T<:BlockIterator}(bi::T, status) = (0 == status)

function next{T<:BlockableIO}(bi::BlockAffinityIterator{T}, status)
    blk = bi.b
    data = blk.affinity
    status = eof(blk.source) ? 0 : (status+1)
    (data, status)
end
function next{T<:Any}(bi::BlockAffinityIterator{T}, status)
    blk = bi.b
    data = isempty(blk.affinity) ? blk.affinity : blk.affinity[status]
    status = (status >= length(blk.block)) ? 0 : (status+1)
    (data, status)
end

function next{T<:BlockableIO}(bi::BlockDataIterator{T}, status)
    blk = bi.b
    data = blk.prepare(blk.block[1])
    status = eof(blk.source) ? 0 : (status+1)
    (data, status)
end
function next{T<:Any}(bi::BlockDataIterator{T}, status)
    blk = bi.b
    data = blk.prepare(blk.block[status])
    status = (status >= length(blk.block)) ? 0 : (status+1)
    (data, status)
end

blocks(b::Block) = BlockDataIterator(b)
affinities(b::Block) = BlockAffinityIterator(b)

# Returns blocks that have affinity for local process id.
# If blocks have no affinity, returns a subset after equally dividing the blocks among processors.
# Note: if blocks have affinity with multiple processors, this does not ensure an equal distribution.
localpart(blk::Block) = localpart(blk, myid())
function localpart(blk::Block, pid)
    blks = collect(blocks(blk))
    (nworkers() == 1) && return blks

    local_blocks = Any[]
    affs = collect(affinities(blk))
    if isempty(affs) || isempty(affs[1])
        # pick up only some parts corresponding to pid
        n = pid-1
        while n <= length(blks)
            push!(local_blocks, blks[n])
            n += nworkers()
        end
    else
        for n in 1:length(affs)
            (pid in affs[n]) && push!(local_blocks, blks[n])
        end
    end
    local_blocks
end

##
# Block implementations for common types

function Block(a::Array, by::Int=0, nsplits::Int=0)
    asz = size(a)
    (by == 0) && (by = length(asz))
    (nsplits == 0) && (nsplits = nworkers())
    sz = [1:x for x in asz]
    splits = map((x)->begin sz[by]=x; tuple(sz...); end, Base.splitrange(length(sz[by]), nsplits))
    #blks = [slice(a,x) for x in splits]
    blks = [a[x...] for x in splits]
    Block(a, blks, no_affinity, as_it_is, as_it_is)
end

function Block(A::Array, dims::Array=[])
    isempty(dims) && error("no dimensions specified")
    dimsA = [size(A)...]
    ndimsA = ndims(A)
    alldims = [1:ndimsA;]

    blks = Any[A]
    if dims != alldims
        otherdims = setdiff(alldims, dims)
        idx = fill!(cell(ndimsA), 1)

        Asliceshape = tuple(dimsA[dims]...)
        itershape   = tuple(dimsA[otherdims]...)
        for d in dims
            idx[d] = 1:size(A,d)
        end
        numblocks = *(itershape...)
        blks = cell(numblocks)
        blkid = 1
        if VERSION >= v"0.4.0-dev+3184"
            for idxs in CartesianRange(itershape)
                idx[otherdims] = [idxs[i] for i in 1:length(idxs)]
                blks[blkid] = reshape(A[idx...], Asliceshape)
                blkid += 1
            end
        else
            cartesianmap(itershape) do idxs...
                idx[otherdims] = [idxs...]
                blks[blkid] = reshape(A[idx...], Asliceshape)
                blkid += 1
            end
        end
    end
    Block(A, blks, no_affinity, as_it_is, as_it_is)
end


function Block(f::File, nsplits::Int=0)
    sz = filesize(f.path)
    (sz == 0) && error("missing or empty file: $(f.path)")
    (nsplits == 0) && (nsplits = nworkers())
    splits = Base.splitrange(@compat(Int(sz)), nsplits)
    data = [(f.path, x) for x in splits]
    Block(f, data, no_affinity, as_it_is, as_it_is)
end

function Block(f::File, recurse::Bool=true, nfiles_per_split::Int=0)
    files = AbstractString[]
    all_files(f.path, recurse, files)

    data = Any[]
    (nfiles_per_split == 0) && (nfiles_per_split = iceil(length(files)/nworkers()))

    while length(files) > 0
        np = min(nfiles_per_split, length(files))
        push!(data, files[1:np])
        files = files[(np+1):end]
    end

    Block([f], data, no_affinity, as_it_is, as_it_is)
end

Block(f::File) = isdir(f.path) ? Block(f, true, 0) : Block(f, 0)

Block{T<:BlockableIO}(aio::T, maxsize::Int) = Block(aio, [(aio,maxsize)], no_affinity, as_it_is, as_it_is)
Block{T<:BlockableIO}(aio::T, approxsize::Int, dlm::Char) = Block(aio, [(aio,approxsize,dlm)], no_affinity, as_it_is, as_it_is)


##
# utility methods
function all_files(path::AbstractString, recurse::Bool, list::Array)
    files = readdir(path)

    for file in files
        fullpath = joinpath(path, file)
        if isdir(fullpath) && recurse
            all_files(fullpath, true, list)
        else
            push!(list, fullpath)
        end
    end
    nothing
end

