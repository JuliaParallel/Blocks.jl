
# Blocked operations on matrices
type MatOpBlock
    m1::Matrix
    m2::Matrix
    oper::Symbol
    splits1::Tuple
    splits2::Tuple

    function MatOpBlock(m1::Matrix, m2::Matrix, oper::Symbol, np::Int=0)
        (0 == np) && (np = nprocs())
        (blks, affs) = (oper == :*) ? matop_block_mul(m1, m2, np) : error("operation $oper not supported")
        new(m1, m2, oper, blks, affs)
    end
end

function Blocks(mb::MatOpBlock)
    (mb.oper == :*) && return block_mul(mb)
    error("operation $(mb.oper) not supported")
end

op{T<:MatOpBlock}(blk::Blocks{T}) = (eval(blk.source.oper))(blk)

##
# internal methods
function common_factor_around(around::Int, nums::Int...)
    gf = gcd(nums...)
    ((gf == 1) || (gf < around)) && return gf

    factors = Int[]

    n = int(floor(gf/around))+1
    while n < gf
        (0 == (gf%n)) && (push!(factors, int(gf/n)); break)
        n += 1
    end

    n = int(floor(gf/around))
    while n > 0
        (0 == (gf%n)) && (push!(factors, int(gf/n)); break)
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

function matop_block_mul(m1::Matrix, m2::Matrix, np::Int)
    s1 = size(m1)
    s2 = size(m2)
    
    fc = common_factor_around(int(min(s1[2],s2[1])/np), s1[2], s2[1])
    f1 = common_factor_around(int(s1[1]/np), s1[1])
    f2 = common_factor_around(int(s2[2]/np), s2[2])

    splits1 = mat_split_ranges(s1, int(s1[1]/f1), int(s1[2]/fc)) # splits1 is f1 x fc blocks
    splits2 = mat_split_ranges(s2, int(s2[1]/fc), int(s2[2]/f2)) # splits2 is fc x f2 blocks
    (tuple(splits1...), tuple(splits2...))
end

function block_mul(mb::MatOpBlock)
    m1 = mb.m1
    m2 = mb.m2
    m1size = size(m1)
    m2size = size(m2)

    blklist = {}
    afflist = {}
    proclist = workers()
    # distribute by splits on m1
    for idx1 in 1:length(mb.splits1)
        split1 = mb.splits1[idx1]
        proc = shift!(proclist)
        push!(proclist, proc)
        for idx2 in 1:length(mb.splits2)
            split2 = mb.splits2[idx2]
            (split1[2] != split2[1]) && continue
            resranges = (split1[1], split2[2])
            #println("m1$split1 x m2$split2 = res$resranges")
            # TODO: need more tricks to send a block only once to a node
            push!(blklist, (resranges, m1[split1...], m2[split2...]))
            # set affinities of all blocks each split of m1 needs to same processor
            push!(afflist, proc)
        end
    end

    Blocks(mb, blklist, afflist, as_it_is)
end

##
# operations
function *{T<:MatOpBlock}(blk::Blocks{T})
    mb = blk.source
    m1 = mb.m1
    m2 = mb.m2
    m1size = size(m1)
    m2size = size(m2)
    restype = typeof(m1[1] * m2[1])
    res = zeros(restype, m1size[1], m2size[2])

    pmapreduce((t)->(t[1], t[2]*t[3]), (v,t)->begin v[t[1]...] += t[2]; v; end, res, blk)
    #pmapreduce((t)->begin println("on $(myid()) res$(t[1])"); (t[1], t[2]*t[3]); end, (v,t)->begin v[t[1]...] += t[2]; v; end, res, blk)
    res
end

