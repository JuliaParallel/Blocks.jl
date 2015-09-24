function filtered(filters::Vector{Function}, b...)
    np = length(b)
    [(filters[idx])(b[idx]) for idx in 1:np]
end
function filtered_fn_call(m::Function, filters::Vector{Function}, b...)
    filt = filtered(filters, b...)
    m(filt...)
end

function map(m::Base.Callable, bf::Block...)
    blks = [blocks(x) for x in bf]
    filters = [x.filter for x in bf]
    fc = (b...)->filtered_fn_call(m, filters, b...)
    map(fc, blks...)
end

function pmap(m, bf::Block...; kwargs...)
    affs = [x.affinity for x in bf]
    filters = [x.filter for x in bf]
    fc = (b...)->filtered_fn_call(m, filters, b...)
    # without any affinities, fall back to default pmap
    (sum([((x == no_affinity) ? 0 : 1) for x in affs]) == 0) && (return block_pmap(fc, [blocks(b) for b in bf]...; kwargs...))

    blks = [x.block for x in bf]
    prepare = [x.prepare for x in bf]

    n1 = length(affs[1])
    n = length(affs)

    # check if all parts are of same number of blocks
    for idx in 2:n
        (length(affs[idx]) != n1) && error("all parts must be of the same number of blocks")
    end

    afflist = [intersect([affs[x][j] for x in 1:n]...) for j in 1:n1]

    # check if at least one common processor for each block
    for idx in 1:n1
        isempty(afflist[idx]) && error("no common processor for block $(idx)")
    end

    donelist = falses(n1)
    np = nworkers()
    results = cell(n1)

    # function to produce the next work item from the queue.
    # in this case it's just an index.
    nextidx(p) = begin
        for (idx,a) in enumerate(afflist)
            if !donelist[idx] && (p in a)
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
        wrkrs = workers()
        for p=1:np
            wpid = wrkrs[p]
            @async begin
                while true
                    idx = nextidx(wpid)
                    (idx == 0) && break
                    #println("processor $(wpid) got idx $idx ... preparing data...")
                    d = [(prepare[bid])((blks[bid])[idx]) for bid in 1:length(blks)]
                    #println("processor $(wpid) preapred idx $idx")
                    results[idx] = fetch_results ? remotecall_fetch(fc, wpid, d...) : remotecall_wait(fc, wpid, d...)
                end
            end
        end
    end
    results
end


pmapreduce(m, r, bf::Block...) = reduce(r, pmap(m, bf...))
function pmapreduce(m, r, v0, bf::Block...)
    results = pmap(m, bf...)
    ret = v0
    for res in results
        ret = r(ret, res)
    end
    ret
    #reduce(r, v0, pmap(m, bf...))
end

