const datafile = "test.csv"
const nloops = 10

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

df = dreadtable(datafil, header=false)
colnames(df)
colnames!(df, ["c1","c2","c3","c4","c5","c6","c7","c8","c9","c10"]
@assert df+df == 2*df
res = f_df1()
println("results: $(res)")

res = f_df2()
println("results: $(res)")

