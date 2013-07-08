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

#f_df() = pmap(x->begin println(x); nrow(x); end, Blocks(File(datafile), DataFrame, 4); header=true)
f_df1() = pmap(x->nrow(x), Blocks(File(datafile), DataFrame); header=false)
f_df2() = pmap(x->nrow(x), Blocks(File(datafile), DataFrame); header=(false,["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10"]))

function load_pkgs()
    println("loading packages...")
    @everywhere using Block
    @everywhere using Base.FS
    @everywhere using DataFrames
end

println("adding 3 more processors...")
addprocs(3)
println("\tnprocs: $(nprocs())")
load_pkgs()

res = f_df1()
println("results: $(res)")

res = f_df2()
println("results: $(res)")

