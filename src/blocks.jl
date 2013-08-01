abstract AbstractBlocks{T,D}
typealias BlockableIO Union(IOStream,AsyncStream,IOBuffer,BlockIO)

# Blocks can chunk content that is:
#   - non-streaming
#   - accessible in parallel
#   - distributed across processors
type Blocks{T} <: AbstractBlocks{T}
    source::T
    block::Array            # chunk definition
    affinity::Array         # chunk affinity
    filter::Function        # returns a processed block. can be chained. default just passes the chunk definition from "block"
    prepare::Function
end

const no_affinity = []

##
# Pipelined filters
# TODO: relook at IO integration after IO enhancements in base
as_it_is(x) = x
function as_io(x::Tuple) 
    if isa(x[1], AsyncStream)
        aio = x[1]
        maxsize = x[2]
        start_reading(aio)
        Base.wait_readnb(aio, maxsize)
        avlb = min(nb_available(aio.buffer), maxsize)
        return PipeBuffer(read(aio, Array(Uint8,avlb)))
    elseif isa(x[1], BlockableIO)
        io = x[1]
        # using BlockIO to avoid copy. 
        # but since BlockIO would skip till the first record delimiter, 
        # we must present it the last read byte (which must be the last read delimiter)
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
    buff = read(x, Array(Uint8, nb_available(x)))
    close(x)
    IOBuffer(buff)
end
as_lines(x::BlockableIO) = readlines(x)
as_bytearray(x::BlockableIO) = read(x, Array(Uint8, nb_available(x)))

as_recordio(x::BlockIO, dlm::Char='\n') = BlockIO(x, dlm)
function as_recordio(x::Tuple)
    dlm = (length(x) == 3) ? x[3] : '\n'
    if isa(x[1], AsyncStream)
        aio = x[1]
        minsize = x[2]
        start_reading(aio)
        Base.wait_readnb(aio, minsize)
        tot_avlb = nb_available(aio.buffer)
        avlb = min(tot_avlb, minsize)
        pb = PipeBuffer(read(aio, Array(Uint8,avlb)))
        write(pb, readuntil(aio, dlm))
        return pb
    else
        return BlockIO(as_io(x), dlm)
    end
end

|>(b::Blocks, f::Function) = filter(f, b)
function filter(f::Function, b::Blocks)
    let oldf = b.filter
        b.filter = (x)->f(oldf(x))
        return b
    end
end

function filter(flist::Vector{Function}, b::Blocks)
    for f in flist
        filter(f, b)
    end
    b
end

.>(b::Blocks, f::Function) = prepare(f, b)
function prepare(f::Function, b::Blocks)
    let oldf = b.prepare
        b.prepare = (x)->f(oldf(x))
        return b
    end
end

function prepare(flist::Vector{Function}, b::Blocks)
    for f in flist
        prepare(f, b)
    end
    b
end

##
# Iterators for blocks and affinities
abstract BlocksIterator{T}
type BlocksAffinityIterator{T} <: BlocksIterator{T}
    b::Blocks{T}
end
type BlocksDataIterator{T} <: BlocksIterator{T}
    b::Blocks{T}
end
start{T<:BlocksIterator}(bi::T) = 1
done{T<:BlocksIterator}(bi::T,status) = (0 == status)
function next{T<:BlockableIO}(bi::BlocksIterator{T},status)
    blk = bi.b
    data = isa(bi, BlocksAffinityIterator) ? blk.affinity : blk.prepare(blk.block[1])
    status = eof(blk.source) ? 0 : (status+1)
    (data,status)
end
function next{T<:Any}(bi::BlocksIterator{T},status)
    blk = bi.b
    data = isa(bi, BlocksAffinityIterator) ? (isempty(blk.affinity) ? blk.affinity : blk.affinity[status]) : blk.prepare(blk.block[status])
    status = (status >= length(blk.block)) ? 0 : (status+1)
    (data,status)
end

blocks(b::Blocks) = BlocksDataIterator(b)
affinities(b::Blocks) = BlocksAffinityIterator(b)


##
# Blocks implementations for common types

function Blocks(a::Array, by::Int=0, nsplits::Int=0)
    asz = size(a)
    (by == 0) && (by = length(asz))
    (nsplits == 0) && (nsplits = nworkers())
    sz = [1:x for x in asz]
    splits = map((x)->begin sz[by]=x; tuple(sz...); end, Base.splitrange(length(sz[by]), nsplits))
    #blocks = [slice(a,x) for x in splits]
    blocks = [a[x...] for x in splits]
    Blocks(a, blocks, no_affinity, as_it_is, as_it_is)
end

function Blocks(A::Array, dims::Array=[])
    isempty(dims) && error("no dimensions specified")
    dimsA = [size(A)...]
    ndimsA = ndims(A)
    alldims = [1:ndimsA]

    blocks = {A}
    if dims != alldims
        otherdims = setdiff(alldims, dims)
        idx = fill!(cell(ndimsA), 1)

        Asliceshape = tuple(dimsA[dims]...)
        itershape   = tuple(dimsA[otherdims]...)
        for d in dims
            idx[d] = 1:size(A,d)
        end
        numblocks = *(itershape...)
        blocks = cell(numblocks)
        blkid = 1
        cartesianmap(itershape) do idxs...
            ia = [idxs...]
            idx[otherdims] = ia
            blocks[blkid] = reshape(A[idx...], Asliceshape)
            blkid += 1
        end
    end
    Blocks(A, blocks, no_affinity, as_it_is, as_it_is)
end

function Blocks(f::File, nsplits::Int=0)
    sz = filesize(f.path)
    (sz == 0) && error("missing or empty file: $(f.path)")
    (nsplits == 0) && (nsplits = nworkers())
    splits = Base.splitrange(sz, nsplits)
    data = [(f.path, x) for x in splits]
    Blocks(f, data, no_affinity, as_it_is, as_it_is)
end

Blocks{T<:BlockableIO}(aio::T, maxsize::Int) = Blocks(aio, [(aio,maxsize)], no_affinity, as_it_is, as_it_is)
Blocks{T<:BlockableIO}(aio::T, approxsize::Int, dlm::Char) = Blocks(aio, [(aio,approxsize,dlm)], no_affinity, as_it_is, as_it_is)

