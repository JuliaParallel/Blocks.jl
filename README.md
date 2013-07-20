Blocks.jl
=========
A framework to:
- represent chunks of entities
- represent processor affinities of the chunks
- compose actions on chunks by chaining functions 
- do map and reduce operations with the above

As examples of its utility, it has been used to implement chunked and distributed operations on disk files, HDFS files, arrays, and dataframes.

### Creating Blocks
#### Disk Files
````
    Blocks(file::File, nblocks::Int=0)
        Where nblocks is the number of chunks to divide the file into (defaults to number of worker processes).
        Each chunk is represented as the file and the byte range.
        Assumes that the file is available at all processors and chunks can be processed anywhere.
````

#### HDFS Files
````
    Blocks(file::HdfsURL)
        Each chunk is a block in HDFS.
        Processor affinity of each chunk is set to machines where this block has been replicated by HDFS.
````

#### Arrays:
````
    Blocks(A::Array, dims::Array)
        Chunks created across dimensions specified in dims.
        Chunks are not pre-distributed and any chunk can be processed at any processor.

    Blocks(A::Array, dim::Int, nblocks::Int)
        Chunked to nblocks chunks on dimension dim.
        Chunks are not pre-distributed and any chunk can be processed at any processor.
````

#### Distributed DataFrames:
Blocks introduces a distributed `DataFrame` type named `DDataFrame`. It holds referenced to multiple remote data frames, on multiple processors. A large table can be read in parallel into a DDataFrame by using the special `dreadtable` method. 

````
    dreadtable(filename::String; kwargs...)
    dreadtable(blocks::Blocks; kwargs...)
        Where blocks are created from disk or HDFS files as described in sections above.
````

A `DDataFrame` is easily represented as Blocks. `DDataFrame` has been used with `Blocks` to implement most `DataFrame` operations in a distributed manner. Most methods defined on a DataFrame also work on DDataFrames in a distributed manner using `pmap` and `reduce` to operate on chunks parallely.

````
julia> dt = dreadtable("test.csv")
100x10 DDataFrame. 2 blocks over 2 processors

julia> head(dt)
6x10 DataFrame:
               x1        x2        x3        x4        x5        x6        x7       x8       x9      x10
[1,]     0.105518  0.173988  0.244224 0.0174508 0.0969595   0.12792  0.316974 0.852373 0.165014 0.886957
[2,]     0.319401 0.0719447 0.0019209  0.285511  0.945343  0.926718  0.162048 0.118748 0.361014 0.611316
[3,]     0.516926  0.473779  0.867099  0.408605  0.579969  0.111174 0.0790296 0.263822 0.073827 0.187637
[4,]     0.579538  0.319672  0.600223  0.707782  0.806437  0.402244  0.670792  0.10981 0.518356 0.604807
[5,]     0.660944  0.648076  0.611529  0.885457  0.550101 0.0634721  0.152263 0.855182 0.408393 0.473676
[6,]    0.0324734   0.22839  0.812387   0.59965  0.143703    0.1337  0.945763 0.296137 0.875762 0.989037

julia> colsums(dt)
1x10 DataFrame:
             x1      x2      x3      x4      x5      x6     x7      x8      x9     x10
[1,]    46.1597 41.9286 51.4197 50.1906 48.2623 44.5622 50.914 50.7266 44.1346 51.1001

julia> all(dt+dt .== 2\*dt)
true
````

### Composing Actions on Blocks
Functions can be chained and then applied on to chunks in a block with a `pmap` or `pmapreduce`. The Julia notation `|>` is used to indicate chaining. For example to read a block of DataFrame from a chunk of a disk file:
````
b = Blocks(File(filename) |> as\_io |> as\_recordio |> as\_dataframe
````
Each function in the chain works on the output of the previous function.


### Map and Reduces on Blocks
Regular Julia map-reduce methods can be used on blocks. The map methods receive the chunks as they have been processed by the chain of actions composed into the Blocks.

````
julia> ba = Blocks([1:100], 1, 10);

julia> pmap(x->sum(x), ba)
10-element Any Array:
  55
 155
 255
 355
 455
 555
 655
 755
 855
 955

julia> pmapreduce(x->sum(x), +, ba)
5050

julia> ba = Blocks([1:100], 1, 10);

julia> map(x->sum(x), ba)
10-element Any Array:
  55
 155
 255
 355
 455
 555
 655
 755
 855
 955

julia> mapreduce(x->sum(x), +, ba)
5050
````

