using Blocks
using Compat

const datafile = joinpath(dirname(@__FILE__), "test.csv")
const nloops = 10

function testfn(f::Function, s::AbstractString, exp_res)
    println("\t$(s)...")
    ret = f()
    println("\t\tresult: $(ret)")
    @assert (ret == exp_res)
    println("\t\tresult: $(ret)")

    t = @elapsed for i in 1:nloops f(); end
    println("\t\ttime: $(t/nloops)")
end

#f_df_pmap() = pmapreduce(x->nrow(x), +, Block(File(datafile)) |> as_io |> as_recordio |> (x)->as_dataframe(x; header=false))
#f_df_map() = sum(map(x->nrow(x), Block(File(datafile)) |> as_io |> as_recordio |> (x)->as_dataframe(x; header=false)))
a_a1_pmap() = pmapreduce(x->sum(2*x), +, Block([1:1000;], 1, nworkers()))
a_a1_map() = sum(map(x->sum(2*x), Block([1:1000;], 1, nworkers())))
a_a2_pmap() = pmapreduce(x->sum(2*x), +, Block(reshape([1:1000;],10,10,10), [1,2]))
a_a2_map = ()->sum(map(x->sum(2*x), Block(reshape([1:1000;],10,10,10), [1,2])))
f_ios_pmap() = pmapreduce(x->length(x), +, Block(File(datafile)) |> as_io |> as_recordio |> as_lines)
f_ios_map() = sum(map(x->length(x), Block(File(datafile)) |> as_io |> as_recordio |> as_lines))
f_stream_pmap() = pmapreduce(x->length(x), +, @prepare Block(open(datafile), 1000) |> as_recordio |> as_lines)
f_stream_map() = sum(map(x->length(x), @prepare Block(open(datafile), 1000) |> as_recordio |> as_lines))

function procaff()
    b = Block(File(datafile)) |> as_io |> as_recordio |> as_lines
    # create random affinities
    wrkrids = workers()
    #nw = length(wrkrids)
    b.affinity = shuffle!(wrkrids) #map(x->randsample(wrkrids, min(3,nw)), 1:nw)
    pmapreduce(x->length(x), +, b)
end

function load_pkgs()
    println("loading packages...")
    @everywhere using Blocks
    @everywhere using Base.FS
end

function do_all_tests()
    println("running tests...")
    #testfn(f_df_pmap, "pmap file->dataframe", 100)
    #testfn(f_df_map, "map file->dataframe", 100)
    testfn(a_a1_pmap, "pmap array->array", 1001000)
    testfn(a_a1_map, "map array->array", 1001000)
    testfn(a_a2_pmap, "pmap array->array", 1001000)
    testfn(a_a2_map, "map array->array", 1001000)
    testfn(f_ios_pmap, "pmap file->iostream", 100)
    testfn(f_ios_map, "map file->iostream", 100)
    testfn(f_stream_pmap, "pmap stream->lines", 100)
    testfn(f_stream_map, "map stream->lines", 100)
    testfn(procaff, "pmap processor affinity", 100)
end

load_pkgs()
do_all_tests()

println("adding 3 more processors...")
addprocs(3)
println("\tnworkers: $(nworkers())")
load_pkgs()
do_all_tests()

