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

function _mf_file_io(m::Function, x::Tuple)
    bio = BlockIO(open(x[1]), x[2])
    m(bio)
end

pmap{T<:File,D<:IO}(m, bf::Blocks{T,D}...) = pmap_base(x->_mf_file_io(m,x), bf...)
pmap{T<:Array,D<:Array}(m, bf::Blocks{T,D}...) = pmap_base(m, bf...)

pmapreduce(m, r, bf::Blocks...) = reduce(r, pmap(m, bf...))
pmapreduce(m, r, v0, bf::Blocks...) = reduce(r, v0, pmap(m, bf...))

