using Base.Test
const NPROCS = isempty(ARGS) ? 4 : parse(Int, ARGS[1])


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
    @test_approx_eq tr result

    println("times: local:$t2 parallel:$t1")
end

function do_mul_large()
    println("creating initial data...")
    a1 = reshape([[1:5000;];[1:5000;]], 1, 10000)
    a2 = reshape([[1:3000;];[1:3000;]], 6000, 1)
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
    @test_approx_eq tr result

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

function do_rand_mul(N=24000)
    println("creating initial data...")
    a1 = RandomMatrix(Float64, (N, N), 0)
    a2 = RandomMatrix(Float64, (N, 1), 0)

    println("making blocks...")
    mb = MatOpBlock(a1, a2, :*, NPROCS)
    blk = Block(mb)
    println("running matrix operation...")
    t1 = @elapsed (result = op(blk))
    #t2 = 0
    #rmprocs(workers())

    println("calculating locally...")
    la1 = convert(DenseMatrix, mb.mb1)
    la2 = convert(DenseMatrix, mb.mb2)
    t2 = @elapsed (tr = la1*la2)

    println("verifying...")
    @test_approx_eq tr result

    println("times: local:$t2 parallel:$t1")
end

function do_sparse_mul(N=5000)
    println("creating initial data...")
    refs = RemoteRef[remotecall(sprand, x, N, N, 0.1) for x in workers()]
    a1 = DSparseMatrix(Float64, (N, N), refs)
    v1 = rand(N, 1)

    println("making blocks...")
    mb = MatOpBlock(a1, v1, :*, NPROCS)
    blk = Block(mb)
    println("running matrix operation...")
    t1 = @elapsed (result = op(blk))

    la1 = spzeros(N, N)
    for ref in refs
        la1 = la1 .+ fetch(ref)
    end
    t2 = @elapsed (tr = la1 * v1)

    println("verifying...")
    @test_approx_eq tr result
    
    println("times: local:$t2 parallel:$t1")
end

println("adding $NPROCS more processors...")
addprocs(NPROCS)
println("\tnworkers: $(nworkers())")
println("loading packages...")
using Blocks
using Blocks.MatOp

do_mul()
#do_mul_large()
#do_mul_test()
#do_rand_mul()
#do_sparse_mul()
