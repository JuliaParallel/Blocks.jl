type Blocks{T,D}
    source::T
    outtype::Type{D}
    block::Array
    affinity::Array
end

const no_affinity = []

function Blocks(a::Array, outtype::Type, by::Int=0, nsplits::Int=0)
    asz = size(a)
    (by == 0) && (by = length(asz))
    (nsplits == 0) && (nsplits = nworkers())
    sz = map(x->1:x, [asz...])
    splits = map((x)->begin sz[by]=x; tuple(sz...); end, Base.splitrange(length(sz[by]), nsplits))
    blocks = [slice(a,x) for x in splits]
    Blocks(a, outtype, blocks, no_affinity)
end

function Blocks(A::Array, outtype::Type, dims::Array)
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
    Blocks(A, outtype, blocks, no_affinity)    
end

function Blocks(f::File, outtype::Type, nsplits::Int=0)
    sz = filesize(f.path)
    (nsplits == 0) && (nsplits = nworkers())
    splits = Base.splitrange(sz, nsplits)
    data = [(f.path, x) for x in splits]
    Blocks(f, outtype, data, no_affinity)
end

function pmap_base{T<:Any,D<:Any}(m, bf::Blocks{T,D}...)
    affinities = [x.affinity for x in bf]
    blks = [x.block for x in bf]
    # without any affinities, fall back to default pmap
    (sum([((x == no_affinity) ? 0 : 1) for x in affinities]) == 0) && return pmap(m, blks...)

    (length(bf) > 1) && error("pmap across multiple block lists with affinities not supported (TBD)")

    blklist = blks[1]
    afflist = affinities[1]
    n = length(blklist)
    donelist = falses(n)
    np = nprocs()
    results = cell(n)
    # function to produce the next work item from the queue.
    # in this case it's just an index.
    nextidx(p) = begin
        for (idx,a) in enumerate(afflist)
            if !donelist[idx] && contains(a,p) 
                donelist[idx] = true
                return idx
            end
        end
        0
    end

    @sync begin
        for p=1:np
            wpid = Base.PGRP.workers[p].id
            if wpid != myid() || np == 1
                @async begin
                    while true
                        idx = nextidx(wpid)
                        (idx == 0) && break
                        results[idx] = remotecall_fetch(wpid, m, blklist[idx])
                    end
                end
            end
        end
    end
    results
end

pmap{T<:File,D<:IO}(m, bf::Blocks{T,D}...) = pmap_base(x->m(BlockIO(open(x[1]), x[2])), bf...)
pmap{T<:File,D<:DataFrame}(m, bf::Blocks{T,D}...) = pmap_base(x->m(readtable(BlockIO(open(x[1]), x[2], '\n'), header=(1==x[2].start))), bf...)
pmap{T<:Array,D<:Array}(m, bf::Blocks{T,D}...) = pmap_base(m, bf...)

pmapreduce(m, r, bf::Blocks...) = reduce(r, pmap(m, bf...))
pmapreduce(m, r, v0, bf::Blocks...) = reduce(r, v0, pmap(m, bf...))

