const datafile = "test.csv"
const nloops = 10

function load_pkgs()
    println("loading packages...")
    @everywhere using Block
    #@everywhere using Base.FS
    #@everywhere using DataFrames
end

println("adding 3 more processors...")
addprocs(3)
println("\tnprocs: $(nprocs())")
load_pkgs()

df = dreadtable(datafile, header=false)
colnames(df)
colnames!(df, ["c1","c2","c3","c4","c5","c6","c7","c8","c9","c10"])

sum_result = df+df
mul_result = 2*df
eq_result = sum_result .== mul_result
@assert all(eq_result)

