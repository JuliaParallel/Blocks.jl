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

