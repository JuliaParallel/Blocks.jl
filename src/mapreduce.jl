function filtered(filters::Vector{Function}, b...)
    np = length(b)
    [(filters[idx])(b[idx]) for idx in 1:np]
end
function filtered_fn_call(m::Function, filters::Vector{Function}, b...)
    filt = filtered(filters, b...)
    m(filt...)
end

function map{T<:BlockableIO}(m, bf::Blocks{T}...)
    filters = [x.filter for x in bf]
    blks = [x.block[1] for x in bf]
    ret = {}
    while(reduce((iv,v)->iv&(!eof(v.source)), true, bf))
        push!(ret, filtered_fn_call(m, filters, blks...))
    end
    ret
end
function map{T<:Any}(m, bf::Blocks{T}...)
    blks = [x.block for x in bf]
    filters = [x.filter for x in bf]
    fc = (b...)->filtered_fn_call(m, filters, b...)
    map(fc, blks...)
end

pmap{T<:BlockableIO}(m, bf::Blocks{T}...; kwargs...) = block_pmap(m, [blocks(b) for b in bf]...; kwargs...)
function pmap{T<:Any}(m, bf::Blocks{T}...; kwargs...)
    affinities = [x.affinity for x in bf]
    blks = [x.block for x in bf]
    filters = [x.filter for x in bf]
    fc = (b...)->filtered_fn_call(m, filters, b...)

    # without any affinities, fall back to default pmap
    (sum([((x == no_affinity) ? 0 : 1) for x in affinities]) == 0) && (return block_pmap(fc, blks...; kwargs...))

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

    fetch_results = true
    for (n,v) in kwargs
        (n == :fetch_results) && (fetch_results = v)
    end
    @sync begin
        for p=1:np
            wpid = Base.PGRP.workers[p].id
            if wpid != myid() || np == 1
                @async begin
                    while true
                        idx = nextidx(wpid)
                        (idx == 0) && break
                        d = map(b->b[idx], blks)
                        results[idx] = fetch_results ? remotecall_fetch(wpid, fc, d...) : remotecall_wait(wpid, fc, d...)
                    end
                end
            end
        end
    end
    results
end


pmapreduce(m, r, bf::Blocks...) = reduce(r, pmap(m, bf...))
pmapreduce(m, r, v0, bf::Blocks...) = reduce(r, v0, pmap(m, bf...))

mapreduce(m::Union(DataType,Function), r::Function, bf::Blocks...) = reduce(r, map(m, bf...))
mapreduce(m::Union(DataType,Function), r::Function, v0, bf::Blocks...) = reduce(r, v0, map(m, bf...))

