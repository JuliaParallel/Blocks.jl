
type DDataFrame <: AbstractDataFrame
    rrefs::Vector
    procs::Vector
    nrows::Vector
    ncols::Int
    coltypes::Vector
    colindex::Index

    DDataFrame(rrefs::Vector, procs::Vector) = _dims(new(rrefs, procs))
end

show(io::IO, dt::DDataFrame) = println("DDataFrame. $(length(dt.rrefs)) blocks over $(length(union(dt.procs))) processors")

function Blocks{T<:DataFrame}(f::File, outtype::T, nsplits::Int=0)
    sz = filesize(f.path)
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
function _dims(dt::DDataFrame)
    dt.nrows = pmap(x->nrow(x), Blocks(dt, DataFrame, dt.rrefs, dt.procs))
    #dt.ncols = remotecall_fetch(dt.procs[1], ncol, dt.rrefs[1])
    cnames = remotecall_fetch(dt.procs[1], (x)->colnames(fetch(x)), dt.rrefs[1])
    dt.ncols = length(cnames)
    dt.colindex = Index(cnames)
    dt.coltypes = remotecall_fetch(dt.procs[1], (x)->coltypes(fetch(x)), dt.rrefs[1])
    dt
end

function _mf_file_dataframe(m::Function, x::Tuple; kwargs...)
    r = x[2]
    bio = BlockIO(open(x[1]), r, '\n')

    hdr = (:header,true)
    hdrnames = []
    for (idx,x) in enumerate(kwargs)
        (x[1]==:header) && (hdr=splice!(kwargs, idx); break)
    end

    isa(hdr[2],Tuple) && (hdrnames = (hdr[2])[2]; hdr = (:header,(hdr[2])[1]))
    push!(kwargs, hdr[2] ? (:header,(1==r.start)) : hdr)
    
    tbl = readtable(bio; kwargs...)
    (hdrnames != []) && colnames!(tbl, hdrnames)
    m(tbl)
end


function _ref(x)
    r = RemoteRef()
    put(r,x)
    r
end


# Operations on Distributed DataFrames
# TODO: colmedians, colstds, colvars, colffts, colnorms

for f in [DataFrames.elementary_functions, DataFrames.unary_operators, :copy, :deepcopy]
    @eval begin
        function ($f)(dt::DDataFrame)
            rrefs = pmap(x->_ref(($f)(x)), Blocks(dt, DataFrame, dt.rrefs, dt.procs))
            DDataFrame(rrefs, dt.procs)
        end
    end
end

for f in DataFrames.binary_operators
    @eval begin
        function ($f)(dt::DDataFrame, x::Union(Number, NAtype))
            rrefs = pmap(y->_ref(($f)(y,x)), Blocks(dt, DataFrame, dt.rrefs, dt.procs))
            DDataFrame(rrefs, dt.procs)
        end
        function ($f)(x::Union(Number, NAtype), dt::DDataFrame)
            rrefs = pmap(y->_ref(($f)(x,y)), Blocks(dt, DataFrame, dt.rrefs, dt.procs))
            DDataFrame(rrefs, dt.procs)
        end
    end
end

for (f,_) in DataFrames.vectorized_comparison_operators
    for t in [:Number, :String, :NAtype]
        @eval begin
            function ($f){T <: ($t)}(dt::DDataFrame, x::T)
                rrefs = pmap(y->_ref(($f)(y,x)), Blocks(dt, DataFrame, dt.rrefs, dt.procs))
                DDataFrame(rrefs, dt.procs)
            end
            function ($f){T <: ($t)}(x::T, dt::DDataFrame)
                rrefs = pmap(y->_ref(($f)(x,y)), Blocks(dt, DataFrame, dt.rrefs, dt.procs))
                DDataFrame(rrefs, dt.procs)
            end
        end
    end
    @eval begin
        function ($f)(a::DDataFrame, b::DDataFrame)
            rrefs = pmap((x,y)->_ref(($f)(x,y)), Blocks(a, DataFrame, a.rrefs, a.procs), Blocks(b, DataFrame, b.rrefs, b.procs))
            DDataFrame(rrefs, a.procs)
        end
    end
end

for f in (:colmins, :colmaxs, :colprods, :colsums, :colmeans)
    @eval begin
        function ($f)(dt::DDataFrame)
            ($f)(vcat(pmap(x->($f)(x), Blocks(dt, DataFrame, dt.rrefs, dt.procs))...))
        end
    end
end    

for f in DataFrames.array_arithmetic_operators
    @eval begin
        function ($f)(a::DDataFrame, b::DDataFrame)
            # TODO: check dimensions
            rrefs = pmap((x,y)->_ref(($f)(x,y)), Blocks(a, DataFrame, a.rrefs, a.procs), Blocks(b, DataFrame, b.rrefs, b.procs))
            DDataFrame(rrefs, a.procs)
        end
    end
end

for f in [:all, :any]
    @eval begin
        function ($f)(dt::DDataFrame)
            ($f)(pmap(x->($f)(x), Blocks(dt, DataFrame, dt.rrefs, dt.procs)))
        end
    end
end

function isequal(a::DDataFrame, b::DDataFrame)
    all(pmap((x,y)->isequal(x,y), Blocks(a, DataFrame, a.rrefs, a.procs), Blocks(b, DataFrame, b.rrefs, b.procs)))
end

nrow(dt::DDataFrame) = sum(dt.nrows)
ncol(dt::DDataFrame) = dt.ncols
head(dt::DDataFrame) = remotecall_fetch(dt.procs[1], x->head(fetch(x)), dt.rrefs[1])
tail(dt::DDataFrame) = remotecall_fetch(dt.procs[end], x->tail(fetch(x)), dt.rrefs[end])
colnames(dt::DDataFrame) = dt.colindex.names
function colnames!(dt::DDataFrame, vals) 
    pmap(x->colnames!(x, vals), Blocks(dt, DataFrame, dt.rrefs, dt.procs))
    names!(dt.colindex, vals)
end

for f in [:rename, :rename!]
    @eval begin
        function ($f)(dt::DDataFrame, from, to)
            pmap(x->_ref(($f)(x, from, to)), Blocks(dt, DataFrame, dt.rrefs, dt.procs))
            ($f)(dt.colindex, from, to)
        end
    end
end

coltypes(dt::DDataFrame) = dt.coltypes
index(dt::DDataFrame) = dt.colindex

for f in [:vcat, :hcat, :rbind, :cbind]
    @eval begin
        function ($f)(dt::DDataFrame...)
            rrefs = pmap((x...)->_ref(($f)(x...)), [Blocks(a, DataFrame, a.rrefs, a.procs) for a in dt]...)
            procs = dt[1].procs
            DDataFrame(rrefs, procs)   
        end
    end
end

