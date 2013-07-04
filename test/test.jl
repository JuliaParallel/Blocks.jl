println("starting 3 more processes...")
addprocs(3)
println("nprocs: $(nprocs())")

println("loading packages...")
@everywhere using Block
@everywhere using Base.FS
@everywhere using DataFrames

#if myid() == 1
println("running tests...")
datafile = "test.csv"

pmap(x->nrow(x), Blocks(File(datafile), DataFrame, 4))
pmap(x->sum(2*x), Blocks([1:10000000], Array, 1, 10))
pmap(x->sum(2*x), Blocks(reshape([1:1000],10,10,10), Array, [1,2]))
pmap(x->length(split(readall(x))), Blocks(File(datafile), IO, 2))

b = Blocks(File(datafile), IO, 10)
b.affinity = {[2], [3], [3,4], [2,3,4], [4], [2,4], [2,4], [4], [3,4], [3,4]}
pmap(x->length(split(readall(x))), b)
pmapreduce(x->length(split(readall(x))), +, b)
#end

