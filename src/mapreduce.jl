
function filtered_fn_call(m::Function, filters::Vector{Function}, b...)
    np = length(b)
    filtered = [(filters[idx])(b[idx]) for idx in 1:np]
    m(filtered...)
end

function map{T<:Any}(m, bf::Blocks{T}...)
    blks = [x.block for x in bf]
    filters = [x.filter for x in bf]
    fc = (b...)->filtered_fn_call(m, filters, b...)
    map(fc, blks...)
end

function pmap{T<:Any}(m, bf::Blocks{T}...)
    affinities = [x.affinity for x in bf]
    blks = [x.block for x in bf]
    filters = [x.filter for x in bf]
    fc = (b...)->filtered_fn_call(m, filters, b...)

    # without any affinities, fall back to default pmap
    (sum([((x == no_affinity) ? 0 : 1) for x in affinities]) == 0) && (return pmap(fc, blks...))

    n1 = length(affinities[1])
    n = length(affinities)

    # check if all parts are of same number of blocks
    for idx in 2:n
        (length(affinities[idx]) != n1) && error("all parts must be of the same number of blocks")
    end

    afflist = [intersect([affinities[x][j] for x in 1:n]...) for j in 1:n1]

    # check if atleast one common processor for each block
    for idx in 1:n1
        isempty(afflist[idx]) && error("no common processor for block $(idx)")
    end

    donelist = falses(n1)
    np = nprocs()
    results = cell(n1)

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
                        results[idx] = remotecall_fetch(wpid, fc, map(b->b[idx], blks)...) 
                    end
                end
            end
        end
    end
    results
end

#function _mf_file_io(m::Function, x::Tuple)
#    bio = BlockIO(open(x[1]), x[2])
#    m(bio)
#end
#function _mf_file_lines(m::Function, x::Tuple)
#    bio = BlockIO(open(x[1]), x[2], '\n')
#    m(readlines(bio))
#end 
 
#pmap{T<:File,D<:Vector{String}}(m, bf::Blocks{T,D}...) = pmap_base(x->_mf_file_lines(m,x), bf...)
#pmap{T<:File,D<:IO}(m, bf::Blocks{T,D}...) = pmap_base(x->_mf_file_io(m,x), bf...)
#pmap{T<:Array,D<:Array}(m, bf::Blocks{T,D}...) = pmap_base(m, bf...)


pmapreduce(m, r, bf::Blocks...) = reduce(r, pmap(m, bf...))
pmapreduce(m, r, v0, bf::Blocks...) = reduce(r, v0, pmap(m, bf...))

mapreduce(m::Union(DataType,Function), r::Function, bf::Blocks...) = reduce(r, map(m, bf...))
mapreduce(m::Union(DataType,Function), r::Function, v0, bf::Blocks...) = reduce(r, v0, map(m, bf...))

