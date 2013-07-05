const datafile = "test.csv"
const nloops = 10

function testfn(f::Function, s::String)
    println("\t$(s)...")
    ret = f()
    println("\t\tresult: $(ret)")

    tic();
    for i in 1:nloops
        f();
    end
    t = toc();
    println("\t\ttime: $(t/nloops)")
end

f_df() = pmap(x->nrow(x), Blocks(File(datafile), DataFrame, 4))
a_a1() = pmap(x->sum(2*x), Blocks([1:10000000], Array, 1, 10))
a_a2() = pmap(x->sum(2*x), Blocks(reshape([1:1000],10,10,10), Array, [1,2]))
f_ios() = pmap(x->length(split(readall(x))), Blocks(File(datafile), IO, 2))

function procaffmap()
    b = Blocks(File(datafile), IO, 10)
    b.affinity = {[2], [3], [3,4], [2,3,4], [4], [2,4], [2,4], [4], [3,4], [3,4]}
    pmap(x->length(split(readall(x))), b)
end

function procaffmapred()
    b = Blocks(File(datafile), IO, 10)
    b.affinity = {[2], [3], [3,4], [2,3,4], [4], [2,4], [2,4], [4], [3,4], [3,4]}
    pmapreduce(x->length(split(readall(x))), +, b)
end

function load_pkgs()
    println("loading packages...")
    @everywhere using Block
    @everywhere using Base.FS
    @everywhere using DataFrames
end

function do_all_tests()
    testfn(f_df, "file->dataframe")
    testfn(a_a1, "array->array")
    testfn(a_a2, "array->array")
    testfn(f_ios, "file->iostream")
    if nprocs() == 4
        testfn(procaffmap, "map processor affinity")
        testfn(procaffmapred, "mapreduce processor affinity")
    end
end

load_pkgs()
do_all_tests()

println("adding 3 more processors...")
addprocs(3)
println("\tnprocs: $(nprocs())")
load_pkgs()
do_all_tests()



#pmap(x->length(split(readall(x))), ["test.csv", "test.csv", "test.csv", "test.csv"])
#pmap(x->sum(2*x), [1:1000])
