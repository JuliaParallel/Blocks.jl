const NPROCS = isempty(ARGS) ? 3 : int(ARGS[1])


function load_pkgs()
    println("loading packages...")
    @everywhere using Blocks
    @everywhere using Blocks.MatOp
    #@everywhere using Base.FS
    #@everywhere using DataFrames
end

function do_mul()
    println("creating initial data...")
    a1 = reshape([1:10;], 1, 10)
    a2 = reshape([1:6;], 6, 1)
    a3 = a2*a1
    a4 = a3'

    println("making blocks...")
    mb = MatOpBlock(a3, a4, :*, NPROCS)
    blk = Block(mb)
    println("running matrix operation...")
    t1 = @elapsed (result = op(blk))

    println("calculating locally...")
    t2 = @elapsed (tr = a3*a4)

    println("verifying...")
    @assert all(tr .== result)

    println("times: local:$t2 parallel:$t1")
end

function do_mul_large()
    println("creating initial data...")
    a1 = reshape([[1:5000;],[1:5000;]], 1, 10000)
    a2 = reshape([[1:3000;],[1:3000;]], 6000, 1)
    a3 = a2*a1
    a4 = a3'

    println("making blocks...")
    mb = MatOpBlock(a3, a4, :*, NPROCS)
    blk = Block(mb)
    println("running matrix operation...")
    t1 = @elapsed (result = op(blk))

    println("calculating locally...")
    t2 = @elapsed (tr = a3*a4)

    println("verifying...")
    @assert all(tr .== result)

    println("times: local:$t2 parallel:$t1")
end

function do_mul_test()
    a1 = rand(10000, 2000)
    a2 = rand(2000, 1)

    mb = MatOpBlock(a1, a2, :*, NPROCS)
    blk = Block(mb)
    println("running matrix operation...")
    t1 = @elapsed (result = op(blk))
    println("times: parallel:$t1")
end

function do_rand_mul()
    println("creating initial data...")
    a1 = RandomMatrix(Float64, (24000, 24000), 0)
    a2 = RandomMatrix(Float64, (24000, 1), 0)

    println("making blocks...")
    mb = MatOpBlock(a1, a2, :*, NPROCS)
    blk = Block(mb)
    println("running matrix operation...")
    t1 = @elapsed (result = op(blk))
    #t2 = 0
    #rmprocs(workers())

    println("calculating locally...")
    la1 = convert(DenseMatrix, mb.ms1)
    la2 = convert(DenseMatrix, mb.ms2)
    t2 = @elapsed (tr = la1*la2)

    println("verifying...")
    @test_approx_eq tr result

    println("times: local:$t2 parallel:$t1")
end


println("adding $NPROCS more processors...")
addprocs(NPROCS)
println("\tnprocs: $(nprocs())")
load_pkgs()

do_mul()
#do_mul_large()
#do_mul_test()
#do_rand_mul()
