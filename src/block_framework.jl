typealias BlockableIO Union(IOStream,AsyncStream,IOBuffer,BlockIO)

type Block{T} 
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

|>(b::Block, f::Function) = filter(f, b)
function filter(f::Function, b::Block)
    let oldf = b.filter
        b.filter = (x)->f(oldf(x))
        return b
    end
end

function filter(flist::Vector{Function}, b::Block)
    for f in flist
        filter(f, b)
    end
    b
end

.>(b::Block, f::Function) = prepare(f, b)
function prepare(f::Function, b::Block)
    let oldf = b.prepare
        b.prepare = (x)->f(oldf(x))
        return b
    end
end

function prepare(flist::Vector{Function}, b::Block)
    for f in flist
        prepare(f, b)
    end
    b
end

##
# Iterators for blocks and affinities
abstract BlockIterator{T}
type BlockAffinityIterator{T} <: BlockIterator{T}
    b::Block{T}
end
type BlockDataIterator{T} <: BlockIterator{T}
    b::Block{T}
end
start{T<:BlockIterator}(bi::T) = 1
done{T<:BlockIterator}(bi::T,status) = (0 == status)
function next{T<:BlockableIO}(bi::BlockIterator{T},status)
    blk = bi.b
    data = isa(bi, BlockAffinityIterator) ? blk.affinity : blk.prepare(blk.block[1])
    status = eof(blk.source) ? 0 : (status+1)
    (data,status)
end
function next{T<:Any}(bi::BlockIterator{T},status)
    blk = bi.b
    data = isa(bi, BlockAffinityIterator) ? (isempty(blk.affinity) ? blk.affinity : blk.affinity[status]) : blk.prepare(blk.block[status])
    status = (status >= length(blk.block)) ? 0 : (status+1)
    (data,status)
end

blocks(b::Block) = BlockDataIterator(b)
affinities(b::Block) = BlockAffinityIterator(b)


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
    alldims = [1:ndimsA]

    blks = {A}
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
        cartesianmap(itershape) do idxs...
            ia = [idxs...]
            idx[otherdims] = ia
            blks[blkid] = reshape(A[idx...], Asliceshape)
            blkid += 1
        end
    end
    Block(A, blks, no_affinity, as_it_is, as_it_is)
end

function Block(f::File, nsplits::Int=0)
    sz = filesize(f.path)
    (sz == 0) && error("missing or empty file: $(f.path)")
    (nsplits == 0) && (nsplits = nworkers())
    splits = Base.splitrange(sz, nsplits)
    data = [(f.path, x) for x in splits]
    Block(f, data, no_affinity, as_it_is, as_it_is)
end

Block{T<:BlockableIO}(aio::T, maxsize::Int) = Block(aio, [(aio,maxsize)], no_affinity, as_it_is, as_it_is)
Block{T<:BlockableIO}(aio::T, approxsize::Int, dlm::Char) = Block(aio, [(aio,approxsize,dlm)], no_affinity, as_it_is, as_it_is)

