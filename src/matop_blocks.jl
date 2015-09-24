module MatOp

using Blocks
using Compat

importall Blocks
importall Base

export MatOpBlock, Block, RandomMatrix, op, convert, *

##
# RamdomMatrix: A way to represent distributed memory random matrix that is easier to work with blocks
# Actually just a placeholder for eltype, dims and rng seeds, with a few AbstractMatrix methods defined on it
type RandomMatrix{T} <: AbstractMatrix{T}
    t::Type{T}
    dims::NTuple{2,Int}
    seed::Int
end
eltype{T}(m::RandomMatrix{T}) = T
size(m::RandomMatrix) = m.dims

##
# MatrixSplits hold remote refs of parts of the matrix, along with the matrix definition
type MatrixSplits
    m::AbstractMatrix
    splitrefs::Dict
end


##
# Block definition on DenseMatrix.
# Each part contains the part dimensions.
# Parts are fetched from master node on first use, and their resusable references are stored at master node.
# This is not a terribly useful type, just used as an example implementation.
type DenseMatrixPart
    r::RemoteRef
    split::Tuple
end

function Block(A::DenseMatrix, splits::Tuple)
    msplits = MatrixSplits(A, Dict())
    r = RemoteRef()
    put!(r, msplits)
    blks = [DenseMatrixPart(r, split) for split in splits]
    Block(A, blks, Blocks.no_affinity, as_it_is, as_it_is)
end

##
# Block definition on RandomMatrix.
# Each part contains the type, part dimensions, and rng seed.
# Parts are created locally on first use, and their resusable references are stored at master node
# This is not a terribly useful type, just used as an example implementation.
type RandomMatrixPart
    r::RemoteRef
    t::Type
    split::Tuple
    splitseed::Int
end

function Block(A::RandomMatrix, splits::Tuple)
    msplits = MatrixSplits(A, Dict())
    r = RemoteRef()
    put!(r, msplits)
    splitseeds = A.seed + (1:length(splits))
    blks = [RandomMatrixPart(r, eltype(A), splits[idx], splitseeds[idx]) for idx in 1:length(splits)]
    Block(A, blks, Blocks.no_affinity, as_it_is, as_it_is)
end

function matrixpart_get(blk)
    refpid = blk.r.where # get the pid of the master process
    # TODO: need a way to avoid unnecessary remotecalls after the initial fetch
    remotecall_fetch(()->matrixpart_get(blk, myid()), refpid)
end

function matrixpart_get(blk, pid)
    msplits = fetch(blk.r)
    splitrefs = msplits.splitrefs
    key = (pid, blk.split)
    get(splitrefs, key, nothing)
end

function matrixpart_set(blk, part_ref)
    refpid = blk.r.where # get the pid of the master process
    remotecall_wait(()->matrixpart_set(blk, myid(), part_ref), refpid)
end

function matrixpart_set(blk, pid, part_ref)
    msplits = fetch(blk.r)
    splitrefs = msplits.splitrefs
    key = (pid, blk.split)
    splitrefs[key] = part_ref
    nothing
end

function matrixpart(blk)
    # check at the master process if we already have this chunk, and get its ref
    chunk_ref = matrixpart_get(blk)
    (chunk_ref === nothing) || (return fetch(chunk_ref))

    # create the chunk
    part = matrixpart_create(blk)
    part_ref = RemoteRef()
    put!(part_ref, part)
    # update its reference
    matrixpart_set(blk, part_ref)
    # return the chunk
    part
end

function matrixpart_create(blk::RandomMatrixPart)
    part_size = map(length, blk.split)
    srand(blk.splitseed)
    rand(blk.t, part_size...)
end

function matrixpart_create(blk::DenseMatrixPart)
    splits = fetch(blk.r)
    A = splits.m
    part_range = blk.split
    A[part_range...]
end

convert{T}(::Type{DenseMatrix}, r::Block{RandomMatrix{T}}) = convert(DenseMatrix{T}, r)
function convert{T}(::Type{DenseMatrix{T}}, r::Block{RandomMatrix{T}})
    A = r.source
    ret = zeros(eltype(A), size(A))
    for blk in blocks(r)
        part_range = blk.split
        ret[part_range...] = matrixpart_create(blk)
    end
    ret
end

# Blocked operations on matrices
type MatOpBlock
    mb1::Block
    mb2::Block
    oper::Symbol

    function MatOpBlock(m1::AbstractMatrix, m2::AbstractMatrix, oper::Symbol, np::Int=0)
        s1 = size(m1)
        s2 = size(m2)

        (0 == np) && (np = nworkers())
        np = min(max(s1...), max(s2...), np)

        (splits1, splits2) = (oper == :*) ? matop_block_mul(s1, s2, np) : error("operation $oper not supported")

        mb1 = Block(m1, splits1)
        mb2 = Block(m2, splits2)
        new(mb1, mb2, oper)
    end
end

function Block(mb::MatOpBlock)
    (mb.oper == :*) && return block_mul(mb)
    error("operation $(mb.oper) not supported")
end

op{T<:MatOpBlock}(blk::Block{T}) = (eval(blk.source.oper))(blk)

##
# internal methods
function common_factor_around(around::Int, nums::Int...)
    gf = gcd(nums...)
    ((gf == 1) || (gf < around)) && return gf

    factors = Int[]

    n = @compat(Int(floor(gf/around)))+1
    while n < gf
        (0 == (gf%n)) && (push!(factors, @compat(round(Int,gf/n))); break)
        n += 1
    end

    n = @compat(Int(floor(gf/around)))
    while n > 0
        (0 == (gf%n)) && (push!(factors, @compat(round(Int,gf/n))); break)
        n -= 1
    end
    (length(factors) == 1) && (return factors[1])
    ((factors[2]-around) > (around-factors[1])) ? factors[1] : factors[2]
end

function mat_split_ranges(dims::Tuple, nrsplits::Int, ncsplits::Int)
    row_splits = Base.splitrange(dims[1], nrsplits)
    col_splits = Base.splitrange(dims[2], ncsplits)
    splits = Array(Tuple,0)
    for cidx in 1:ncsplits
        for ridx in 1:nrsplits
            push!(splits, (row_splits[ridx],col_splits[cidx]))
        end
    end
    splits
end

# Input:
#   - two matrices (or their dimensions)
#   - number of processors to split over
# Output:
#   - tuples of ranges to split each matrix into, suitable for block multiplication
function matop_block_mul(s1::Tuple, s2::Tuple, np::Int)
    fc = common_factor_around(round(Int,min(s1[2],s2[1])/np), s1[2], s2[1])
    f1 = common_factor_around(round(Int,s1[1]/np), s1[1])
    f2 = common_factor_around(round(Int,s2[2]/np), s2[2])

    splits1 = mat_split_ranges(s1, round(Int,s1[1]/f1), round(Int,s1[2]/fc)) # splits1 is f1 x fc blocks
    splits2 = mat_split_ranges(s2, round(Int,s2[1]/fc), round(Int,s2[2]/f2)) # splits2 is fc x f2 blocks
    (tuple(splits1...), tuple(splits2...))
end

function block_mul(mb::MatOpBlock)
    blklist = Any[]
    afflist = Any[]
    proclist = workers()
    # distribute by splits on m1
    for blk1 in blocks(mb.mb1)
        split1 = blk1.split
        proc = shift!(proclist)
        push!(proclist, proc)
        for blk2 in blocks(mb.mb2)
            split2 = blk2.split
            (split1[2] != split2[1]) && continue
            result_range = (split1[1], split2[2])
            push!(blklist, (result_range, blk1, blk2, proc))
            # set affinities of all blocks each split of m1 needs to same processor
            push!(afflist, proc)
        end
    end

    Block(mb, blklist, afflist, as_it_is, as_it_is)
end

##
# operations
function *{T<:MatOpBlock}(blk::Block{T})
    mb = blk.source
    m1 = mb.mb1.source
    m2 = mb.mb2.source
    m1size = size(m1)
    m2size = size(m2)
    restype = promote_type(eltype(m1), eltype(m2))
    res = zeros(restype, m1size[1], m2size[2])

    pmapreduce((t)->(t[1], matrixpart(t[2])*matrixpart(t[3])), (v,t)->begin v[t[1]...] += t[2]; v; end, res, blk)
    res
end

end # module MatOP
