
type DDataFrame <: AbstractDataFrame
    rrefs::Vector
    procs::Vector
    nrows::Vector
    ncols::Int
    coltypes::Vector
    colindex::Index

    DDataFrame(rrefs::Vector, procs::Vector) = _dims(new(rrefs, procs))
end

show(io::IO, dt::DDataFrame) = println("$(nrow(dt))x$(ncol(dt)) DDataFrame. $(length(dt.rrefs)) blocks over $(length(union(dt.procs))) processors")

gather(dt::DDataFrame) = reduce((x,y)->vcat(fetch(x), fetch(y)), dt.rrefs) 
#convert(::Type{DataFrame}, dt::DDataFrame) = reduce((x,y)->vcat(fetch(x), fetch(y)), dt.rrefs) 

# internal methods
function _dims(dt::DDataFrame, rows::Bool=true, cols::Bool=true)
    dt.nrows = pmap(x->nrow(fetch(x)), Blocks(dt))
    cnames = remotecall_fetch(dt.procs[1], (x)->colnames(fetch(x)), dt.rrefs[1])
    dt.ncols = length(cnames)
    dt.colindex = Index(cnames)
    dt.coltypes = remotecall_fetch(dt.procs[1], (x)->coltypes(fetch(x)), dt.rrefs[1])
    # propagate the column names
    for nidx in 2:length(dt.procs)
        remotecall(dt.procs[nidx], x->colnames!(fetch(x), cnames), dt.rrefs[nidx])
    end
    dt
end

function as_dataframe(bio::BlockableIO; kwargs...)
    kwargs = _check_readtable_kwargs(kwargs...)
    tbl = readtable(bio; kwargs...)
    tbl
end

as_dataframe(A::Array) = DataFrame(A)

Blocks(dt::DDataFrame) = Blocks(dt, dt.rrefs, dt.procs, as_it_is, as_it_is)

function _check_readtable_kwargs(kwargs...)
    kwargs = {kwargs...}
    for kw in kwargs
        contains([:skipstart, :skiprows], kw[1]) && error("dreadtable does not support $(kw[1])")
    end
    for (idx,kw) in enumerate(kwargs)
        if (kw[1]==:header) 
            (kw[2] != false) && error("dreadtable does not support reading of headers")
            splice!(kwargs, idx)
            break
        end
    end
    push!(kwargs, (:header,false))
    kwargs
end

function dreadtable(b::Blocks; kwargs...)
    kwargs = _check_readtable_kwargs(kwargs...)
    b.affinity = workers()
    rrefs = pmap(x->as_dataframe(x;kwargs...), b; fetch_results=false)
    DDataFrame(rrefs, b.affinity)
end
dreadtable(fname::String; kwargs...) = dreadtable(Blocks(File(fname)) |> as_io |> as_recordio; kwargs...)
function dreadtable(io::Union(AsyncStream,IOStream), chunk_sz::Int, merge_chunks::Bool=true; kwargs...)
    b = (Blocks(io, chunk_sz, '\n') .> as_recordio) .> as_bytearray
    rrefs = pmap(x->as_dataframe(PipeBuffer(x); kwargs...), b; fetch_results=false)
    procs = map(x->x.where, rrefs)

    if merge_chunks
        uniqprocs = unique(procs)
        collected_refs = map(proc->rrefs[find(x->(x==proc), procs)], uniqprocs)
        merging_block = Blocks(collected_refs, collected_refs, uniqprocs, as_it_is, as_it_is)

        vcat_refs = pmap(reflist->vcat([fetch(x) for x in reflist]...), merging_block; fetch_results=false)
        rrefs = vcat_refs
        procs = uniqprocs
    end
    
    DDataFrame(rrefs, procs)
end


# Operations on Distributed DataFrames
# TODO: colmedians, colstds, colvars, colffts, colnorms

for f in [DataFrames.elementary_functions, DataFrames.unary_operators, :copy, :deepcopy, :isfinite, :isnan]
    @eval begin
        function ($f)(dt::DDataFrame)
            rrefs = pmap(x->($f)(fetch(x)), Blocks(dt); fetch_results=false)
            DDataFrame(rrefs, dt.procs)
        end
    end
end

for f in [:without]
    @eval begin
        function ($f)(dt::DDataFrame, p1)
            rrefs = pmap(x->($f)(fetch(x), p1), Blocks(dt); fetch_results=false)
            DDataFrame(rrefs, dt.procs)
        end
    end
end

with(dt::DDataFrame, c::Expr) = vcat(pmap(x->with(fetch(x), c), Blocks(dt))...)
with(dt::DDataFrame, c::Symbol) = vcat(pmap(x->with(fetch(x), c), Blocks(dt))...)

function delete!(dt::DDataFrame, c)
    pmap(x->begin delete!(fetch(x),c); nothing; end, Blocks(dt))
    _dims(dt, false, true)
end

function deleterows!(dt::DDataFrame, keep_inds::Vector{Int})
    # split keep_inds based on index ranges
    split_inds = {}
    beg_row = 1
    for idx in 1:length(dt.nrows)
        end_row = dt.nrows[idx]
        part_rows = filter(x->(beg_row <= x <= (beg_row+end_row-1)), keep_inds) .- (beg_row-1)
        push!(split_inds, remotecall_wait(dt.procs[idx], DataFrame, part_rows))
        beg_row = end_row+1
    end
    dt_keep_inds = DDataFrame(split_inds, dt.procs)
    
    pmap((x,y)->begin DataFrames.deleterows!(fetch(x),y[1].data); nothing; end, Blocks(dt), Blocks(dt_keep_inds))
    _dims(dt, true, false)
end

function within!(dt::DDataFrame, c::Expr)
    pmap(x->begin within!(fetch(x),c); nothing; end, Blocks(dt))
    _dims(dt, false, true)
end

for f in (:isna, :complete_cases)
    @eval begin
        function ($f)(dt::DDataFrame)
            vcat(pmap(x->($f)(fetch(x)), Blocks(dt))...)
        end
    end
end    
function complete_cases!(dt::DDataFrame)
    pmap(x->begin complete_cases!(fetch(x)); nothing; end, Blocks(dt))
    _dims(dt, true, true)
end



for f in DataFrames.binary_operators
    @eval begin
        function ($f)(dt::DDataFrame, x::Union(Number, NAtype))
            rrefs = pmap(y->($f)(fetch(y),x), Blocks(dt); fetch_results=false)
            DDataFrame(rrefs, dt.procs)
        end
        function ($f)(x::Union(Number, NAtype), dt::DDataFrame)
            rrefs = pmap(y->($f)(x,fetch(y)), Blocks(dt); fetch_results=false)
            DDataFrame(rrefs, dt.procs)
        end
    end
end

for (f,_) in DataFrames.vectorized_comparison_operators
    for t in [:Number, :String, :NAtype]
        @eval begin
            function ($f){T <: ($t)}(dt::DDataFrame, x::T)
                rrefs = pmap(y->($f)(fetch(y),x), Blocks(dt); fetch_results=false)
                DDataFrame(rrefs, dt.procs)
            end
            function ($f){T <: ($t)}(x::T, dt::DDataFrame)
                rrefs = pmap(y->($f)(x,fetch(y)), Blocks(dt); fetch_results=false)
                DDataFrame(rrefs, dt.procs)
            end
        end
    end
    @eval begin
        function ($f)(a::DDataFrame, b::DDataFrame)
            rrefs = pmap((x,y)->($f)(fetch(x),fetch(y)), Blocks(a), Blocks(b); fetch_results=false)
            DDataFrame(rrefs, a.procs)
        end
    end
end

for f in (:colmins, :colmaxs, :colprods, :colsums, :colmeans)
    @eval begin
        function ($f)(dt::DDataFrame)
            ($f)(vcat(pmap(x->($f)(fetch(x)), Blocks(dt))...))
        end
    end
end    

for f in DataFrames.array_arithmetic_operators
    @eval begin
        function ($f)(a::DDataFrame, b::DDataFrame)
            # TODO: check dimensions
            rrefs = pmap((x,y)->($f)(fetch(x),fetch(y)), Blocks(a), Blocks(b); fetch_results=false)
            DDataFrame(rrefs, a.procs)
        end
    end
end

for f in [:all, :any]
    @eval begin
        function ($f)(dt::DDataFrame)
            ($f)(pmap(x->($f)(fetch(x)), Blocks(dt)))
        end
    end
end

function isequal(a::DDataFrame, b::DDataFrame)
    all(pmap((x,y)->isequal(fetch(x),fetch(y)), Blocks(a), Blocks(b)))
end

nrow(dt::DDataFrame) = sum(dt.nrows)
ncol(dt::DDataFrame) = dt.ncols
head(dt::DDataFrame) = remotecall_fetch(dt.procs[1], x->head(fetch(x)), dt.rrefs[1])
tail(dt::DDataFrame) = remotecall_fetch(dt.procs[end], x->tail(fetch(x)), dt.rrefs[end])
colnames(dt::DDataFrame) = dt.colindex.names
function colnames!(dt::DDataFrame, vals) 
    pmap(x->colnames!(fetch(x), vals), Blocks(dt))
    names!(dt.colindex, vals)
end
function clean_colnames!(dt::DDataFrame)
    new_names = map(n -> replace(n, r"\W", "_"), colnames(dt))
    colnames!(dt, new_names)
    return
end

for f in [:rename, :rename!]
    @eval begin
        function ($f)(dt::DDataFrame, from, to)
            pmap(x->($f)(fetch(x), from, to), Blocks(dt); fetch_results=false)
            ($f)(dt.colindex, from, to)
        end
    end
end

coltypes(dt::DDataFrame) = dt.coltypes
index(dt::DDataFrame) = dt.colindex

for f in [:vcat, :hcat, :rbind, :cbind]
    @eval begin
        function ($f)(dt::DDataFrame...)
            rrefs = pmap((x...)->($f)([fetch(y) for y in x]...), [Blocks(a) for a in dt]...; fetch_results=false)
            procs = dt[1].procs
            DDataFrame(rrefs, procs)   
        end
    end
end

function merge(dt::DDataFrame, t::DataFrame, bycol, jointype)
    (jointype != "inner") && error("only inner joins are supported")
    
    rrefs = pmap((x)->merge(fetch(x),t), Blocks(dt); fetch_results=false)
    DDataFrame(rrefs, dt.procs)
end

function merge(t::DataFrame, dt::DDataFrame, bycol, jointype)
    (jointype != "inner") && error("only inner joins are supported")
    
    rrefs = pmap((x)->merge(t,fetch(x)), Blocks(dt); fetch_results=false)
    DDataFrame(rrefs, dt.procs)
end

colwise(f::Function, dt::DDataFrame) = error("Not supported. Try colwise variant meant for DDataFrame instead.")
colwise(fns::Vector{Function}, dt::DDataFrame) = error("Not supported. Try colwise variant meant for DDataFrame instead.")
colwise(d::DDataFrame, s::Vector{Symbol}, cn::Vector) = error("Not supported. Try colwise variant meant for DDataFrame instead.")

function colwise(f::Function, r::Function, dt::DDataFrame)
    resarr = pmap((x)->colwise(f,fetch(x)), Blocks(dt))
    combined = hcat(resarr...)
    map(x->r([combined[x, :]...]), 1:size(combined,1))
end
function colwise(fns::Vector{Function}, rfns::Vector{Function}, dt::DDataFrame) 
    nfns = length(fns)
    (nfns != length(rfns)) && error("number of operations must match number of reduce operations")
    resarr = pmap((x)->colwise(fns,fetch(x)), Blocks(dt))
    combined = hcat(resarr...)
    map(x->(rfns[x%nfns=1])([combined[x, :]...]), 1:size(combined,1))
end
function colwise(dt::DDataFrame, s::Vector{Symbol}, reduces::Vector{Function}, cn::Vector)
    nfns = length(s)
    (nfns != length(reduces)) && error("number of operations must match number of reduce operations")
    resarr = pmap((x)->colwise(fetch(x), s, cn), Blocks(dt))
    combined = vcat(resarr...)
    resdf = DataFrame()
    
    for (idx,(colname,col)) in enumerate(combined)
        resdf[colname] = (reduces[idx%nfns+1])(col)
    end
    resdf
end

by(dt::DDataFrame, cols, f::Function) = error("Not supported. Try by variant meant for DDataFrame instead.")
by(dt::DDataFrame, cols, e::Expr) = error("Not supported. Try by variant meant for DDataFrame instead.")
by(dt::DDataFrame, cols, s::Vector{Symbol}) = error("Not supported. Try by variant meant for DDataFrame instead.")
by(dt::DDataFrame, cols, s::Symbol) = error("Not supported. Try by variant meant for DDataFrame instead.")

function by(dt::DDataFrame, cols, f, reducer::Function)
    resarr = pmap((x)->by(fetch(x), cols, f), Blocks(dt))
    combined = vcat(resarr...)
    by(combined, cols, x->reducer(x[end]))
end


dwritetable(path::String, suffix::String, dt::DDataFrame; kwargs...) = pmap(x->begin; fn=joinpath(path, string(myid())*"."*suffix); writetable(fn, fetch(x); kwargs...); fn; end, Blocks(dt))

function writetable(filename::String, dt::DDataFrame, do_gather::Bool=false; kwargs...)
    do_gather && (return writetable(filename, gather(dt); kwargs...))

    hdr = (:header,true)
    hdrnames = []
    for (idx,kw) in enumerate(kwargs)
        (kw[1]==:header) && (hdr=splice!(kwargs, idx); break)
    end
    push!(kwargs, (:header,false))

    basen = basename(filename)
    path = filename[1:(length(filename)-length(basename(filename)))]
    filenames = dwritetable(path, basen, dt, header=false)

    if hdr[2]
        h = DataFrame()
        for cns in colnames(dt) h[cns] = [] end
        writetable(filename, h)
    end
    f = open(filename, hdr[2] ? "a" : "w")

    const lb = 1024*16
    buff = Array(Uint8, lb)
    for fn in filenames
        fp = open(fn)
        while(!eof(fp))
            avlb = nb_available(fp)
            write(f, read(fp, (avlb < lb) ? Array(Uint8, avlb) : buff))
        end
        close(fp)
        rm(fn)
    end
    close(f)
end

