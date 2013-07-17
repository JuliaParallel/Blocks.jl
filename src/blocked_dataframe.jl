
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

function Blocks{T<:DataFrame}(f::File, outtype::T, nsplits::Int=0)
    sz = filesize(f.path)
    (0 == sz) && error("file not found")
    (nsplits == 0) && (nsplits = nworkers())
    splits = Base.splitrange(sz, nsplits)
    data = [(f.path, x) for x in splits]
    Blocks(f, outtype, data, no_affinity)
end

function dreadtable(fname::String; kwargs...)
    fblocks = Blocks(File(fname), DataFrame)
    fblocks.affinity = workers()
    rrefs = pmap(x->_ref(x), fblocks; kwargs...)
    DDataFrame(rrefs, fblocks.affinity)
end

pmap{T<:File,D<:DataFrame}(m, bf::Blocks{T,D}...; kwargs...) = pmap_base(x->_mf_file_dataframe(m,x;kwargs...), bf...)
pmap{T<:DDataFrame,D<:DataFrame}(m, bf::Blocks{T,D}...) = pmap_base((x...)->m([fetch(y) for y in x]...), bf...)

# internal methods
function _dims(dt::DDataFrame, rows::Bool=true, cols::Bool=true)
    dt.nrows = pmap(x->nrow(x), _blk(dt))
    #dt.ncols = remotecall_fetch(dt.procs[1], ncol, dt.rrefs[1])
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

function _mf_file_dataframe(m::Function, x::Tuple; kwargs...)
    r = x[2]
    hdr = (:header,true)
    hdrnames = []
    for (idx,kw) in enumerate(kwargs)
        (kw[1]==:header) && (hdr=splice!(kwargs, idx); break)
    end

    isa(hdr[2],Tuple) && (hdrnames = (hdr[2])[2]; hdr = (:header,(hdr[2])[1]))
    push!(kwargs, hdr[2] ? (:header,(1==r.start)) : hdr)
   
    bio = BlockIO(open(x[1]), r, '\n')
    iob = IOBuffer(read(bio, Array(Uint8, filesize(bio))))
    f = open("ttt_"*string(myid()), "w")
    seekstart(bio)
    write(f, read(bio, Array(Uint8, filesize(bio))))
    close(f)
    tbl = readtable(iob; kwargs...)
    close(iob)

    (hdrnames != []) && colnames!(tbl, hdrnames)
    m(tbl)
end


function _ref(x)
    r = RemoteRef()
    put(r,x)
    r
end

_blk(dt::DDataFrame) = Blocks(dt, DataFrame, dt.rrefs, dt.procs)


# Operations on Distributed DataFrames
# TODO: colmedians, colstds, colvars, colffts, colnorms

for f in [DataFrames.elementary_functions, DataFrames.unary_operators, :copy, :deepcopy, :isfinite, :isnan]
    @eval begin
        function ($f)(dt::DDataFrame)
            rrefs = pmap(x->_ref(($f)(x)), _blk(dt))
            DDataFrame(rrefs, dt.procs)
        end
    end
end

for f in [:without]
    @eval begin
        function ($f)(dt::DDataFrame, p1)
            rrefs = pmap(x->_ref(($f)(x, p1)), _blk(dt))
            DDataFrame(rrefs, dt.procs)
        end
    end
end

with(dt::DDataFrame, c::Expr) = vcat(pmap(x->with(x, c), _blk(dt))...)
with(dt::DDataFrame, c::Symbol) = vcat(pmap(x->with(x, c), _blk(dt))...)

function delete!(dt::DDataFrame, c)
    pmap(x->begin delete!(x,c); nothing; end, _blk(dt))
    _dims(dt, false, true)
end

function deleterows!(dt::DDataFrame, keep_inds::Vector{Int})
    # split keep_inds based on index ranges
    split_inds = {}
    beg_row = 1
    for idx in 1:length(dt.nrows)
        end_row = dt.nrows[idx]
        part_rows = filter(x->(beg_row <= x <= (beg_row+end_row-1)), keep_inds) .- (beg_row-1)
        push!(split_inds, remotecall_fetch(dt.procs[idx], x->_ref(DataFrame(x)), part_rows))
        #push!(split_inds, part_rows)
        beg_row = end_row+1
    end
    dt_keep_inds = DDataFrame(split_inds, dt.procs)
    
    # pmap
    pmap((x,y)->begin DataFrames.deleterows!(x,y[1].data); nothing; end, _blk(dt), _blk(dt_keep_inds))
    # calculate dims again
    _dims(dt, true, false)
end

function within!(dt::DDataFrame, c::Expr)
    pmap(x->begin within!(x,c); nothing; end, _blk(dt))
    _dims(dt, false, true)
end

for f in (:isna, :complete_cases)
    @eval begin
        function ($f)(dt::DDataFrame)
            vcat(pmap(x->($f)(x), _blk(dt))...)
        end
    end
end    
function complete_cases!(dt::DDataFrame)
    pmap(x->begin complete_cases!(x); nothing; end, _blk(dt))
    _dims(dt, true, true)
end



for f in DataFrames.binary_operators
    @eval begin
        function ($f)(dt::DDataFrame, x::Union(Number, NAtype))
            rrefs = pmap(y->_ref(($f)(y,x)), _blk(dt))
            DDataFrame(rrefs, dt.procs)
        end
        function ($f)(x::Union(Number, NAtype), dt::DDataFrame)
            rrefs = pmap(y->_ref(($f)(x,y)), _blk(dt))
            DDataFrame(rrefs, dt.procs)
        end
    end
end

for (f,_) in DataFrames.vectorized_comparison_operators
    for t in [:Number, :String, :NAtype]
        @eval begin
            function ($f){T <: ($t)}(dt::DDataFrame, x::T)
                rrefs = pmap(y->_ref(($f)(y,x)), _blk(dt))
                DDataFrame(rrefs, dt.procs)
            end
            function ($f){T <: ($t)}(x::T, dt::DDataFrame)
                rrefs = pmap(y->_ref(($f)(x,y)), _blk(dt))
                DDataFrame(rrefs, dt.procs)
            end
        end
    end
    @eval begin
        function ($f)(a::DDataFrame, b::DDataFrame)
            rrefs = pmap((x,y)->_ref(($f)(x,y)), _blk(a), _blk(b))
            DDataFrame(rrefs, a.procs)
        end
    end
end

for f in (:colmins, :colmaxs, :colprods, :colsums, :colmeans)
    @eval begin
        function ($f)(dt::DDataFrame)
            ($f)(vcat(pmap(x->($f)(x), _blk(dt))...))
        end
    end
end    

for f in DataFrames.array_arithmetic_operators
    @eval begin
        function ($f)(a::DDataFrame, b::DDataFrame)
            # TODO: check dimensions
            rrefs = pmap((x,y)->_ref(($f)(x,y)), _blk(a), _blk(b))
            DDataFrame(rrefs, a.procs)
        end
    end
end

for f in [:all, :any]
    @eval begin
        function ($f)(dt::DDataFrame)
            ($f)(pmap(x->($f)(x), _blk(dt)))
        end
    end
end

function isequal(a::DDataFrame, b::DDataFrame)
    all(pmap((x,y)->isequal(x,y), _blk(a), _blk(b)))
end

nrow(dt::DDataFrame) = sum(dt.nrows)
ncol(dt::DDataFrame) = dt.ncols
head(dt::DDataFrame) = remotecall_fetch(dt.procs[1], x->head(fetch(x)), dt.rrefs[1])
tail(dt::DDataFrame) = remotecall_fetch(dt.procs[end], x->tail(fetch(x)), dt.rrefs[end])
colnames(dt::DDataFrame) = dt.colindex.names
function colnames!(dt::DDataFrame, vals) 
    pmap(x->colnames!(x, vals), _blk(dt))
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
            pmap(x->_ref(($f)(x, from, to)), _blk(dt))
            ($f)(dt.colindex, from, to)
        end
    end
end

coltypes(dt::DDataFrame) = dt.coltypes
index(dt::DDataFrame) = dt.colindex

for f in [:vcat, :hcat, :rbind, :cbind]
    @eval begin
        function ($f)(dt::DDataFrame...)
            rrefs = pmap((x...)->_ref(($f)(x...)), [_blk(a) for a in dt]...)
            procs = dt[1].procs
            DDataFrame(rrefs, procs)   
        end
    end
end

function merge(dt::DDataFrame, t::DataFrame, bycol, jointype)
    (jointype != "inner") && error("only inner joins are supported")
    
    rrefs = pmap((x)->_ref(merge(x,t)), _blk(dt))
    DDataFrame(rrefs, dt.procs)
end

function merge(t::DataFrame, dt::DDataFrame, bycol, jointype)
    (jointype != "inner") && error("only inner joins are supported")
    
    rrefs = pmap((x)->_ref(merge(t,x)), _blk(dt))
    DDataFrame(rrefs, dt.procs)
end

colwise(f::Function, dt::DDataFrame) = error("Not supported. Try colwise variant meant for DDataFrame instead.")
colwise(fns::Vector{Function}, dt::DDataFrame) = error("Not supported. Try colwise variant meant for DDataFrame instead.")
colwise(d::DDataFrame, s::Vector{Symbol}, cn::Vector) = error("Not supported. Try colwise variant meant for DDataFrame instead.")

function colwise(f::Function, r::Function, dt::DDataFrame)
    resarr = pmap((x)->colwise(f,x), _blk(dt))
    combined = hcat(resarr...)
    map(x->r([combined[x, :]...]), 1:size(combined,1))
end
function colwise(fns::Vector{Function}, rfns::Vector{Function}, dt::DDataFrame) 
    nfns = length(fns)
    (nfns != length(rfns)) && error("number of operations must match number of reduce operations")
    resarr = pmap((x)->colwise(fns,x), _blk(dt))
    combined = hcat(resarr...)
    map(x->(rfns[x%nfns=1])([combined[x, :]...]), 1:size(combined,1))
end
function colwise(dt::DDataFrame, s::Vector{Symbol}, reduces::Vector{Function}, cn::Vector)
    nfns = length(s)
    (nfns != length(reduces)) && error("number of operations must match number of reduce operations")
    resarr = pmap((x)->colwise(x, s, cn), _blk(dt))
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
    resarr = pmap((x)->by(x, cols, f), _blk(dt))
    combined = vcat(resarr...)
    by(combined, cols, x->reducer(x[end]))
end


dwritetable(path::String, suffix::String, dt::DDataFrame; kwargs...) = pmap(x->begin; fn=joinpath(path, string(myid())*"."*suffix); writetable(fn, x; kwargs...); fn; end, _blk(dt))

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

