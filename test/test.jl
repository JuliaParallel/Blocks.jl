const datafile = "test.csv"
const nloops = 10

function testfn(f::Function, s::String)
    println("\t$(s)...")
    ret = f()
    println("\t\tresult: $(ret)")

    t = @elapsed for i in 1:nloops f(); end
    println("\t\ttime: $(t/nloops)")
end

f_df_pmap() = pmapreduce(x->nrow(x), +, Block(File(datafile)) |> as_io |> as_recordio |> (x)->as_dataframe(x; header=false))
f_df_map() = sum(map(x->nrow(x), Block(File(datafile)) |> as_io |> as_recordio |> (x)->as_dataframe(x; header=false)))
a_a1_pmap() = pmapreduce(x->sum(2*x), +, Block([1:1000000], 1, nworkers()))
a_a1_map() = sum(map(x->sum(2*x), Block([1:1000000], 1, nworkers())))
a_a2_pmap() = pmapreduce(x->sum(2*x), +, Block(reshape([1:1000],10,10,10), [1,2]))
a_a2_map() = sum(map(x->sum(2*x), Block(reshape([1:1000],10,10,10), [1,2])))
f_ios_pmap() = pmapreduce(x->length(x), +, Block(File(datafile)) |> as_io |> as_recordio |> as_lines)
f_ios_map() = sum(map(x->length(x), Block(File(datafile)) |> as_io |> as_recordio |> as_lines))
f_stream_pmap() = pmapreduce(x->length(x), +, (Block(open(datafile), 1000) .> as_recordio) .> as_lines)
f_stream_map() = sum(map(x->length(x), (Block(open(datafile), 1000) .> as_recordio) .> as_lines))

function procaff()
    b = Block(File(datafile)) |> as_io |> as_recordio |> as_lines
    # create random affinities
    wrkrids = workers()
    nw = length(wrkrids)
    b.affinity = map(x->randsample(wrkrids, min(3,nw)), 1:nw)
    pmapreduce(x->length(x), +, b)
end

function load_pkgs()
    println("loading packages...")
    @everywhere using Blocks
    @everywhere using Blocks.DDataFrames
    @everywhere using Base.FS
    @everywhere using DataFrames
end

function do_all_tests()
    println("running tests...")
    testfn(f_df_pmap, "pmap file->dataframe")
    testfn(f_df_map, "map file->dataframe")
    testfn(a_a1_pmap, "pmap array->array")
    testfn(a_a1_map, "map array->array")
    testfn(a_a2_pmap, "pmap array->array")
    testfn(a_a2_map, "map array->array")
    testfn(f_ios_pmap, "pmap file->iostream")
    testfn(f_ios_map, "map file->iostream")
    testfn(f_stream_pmap, "pmap stream->lines")
    testfn(f_stream_map, "map stream->lines")
    testfn(procaff, "pmap processor affinity")
end

load_pkgs()
do_all_tests()

println("adding 3 more processors...")
addprocs(3)
println("\tnprocs: $(nprocs())")
load_pkgs()
do_all_tests()

