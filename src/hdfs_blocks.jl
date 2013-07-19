
function Blocks(f::HdfsURL, filter::Function=as_it_is)
    worker_ids = workers()
    worker_ips = map(x->getaddrinfo(isa(x, LocalProcess)?getipaddr():x.host), map(x->Base.worker_from_id(x), worker_ids))

    block_dist = hdfs_blocks(f, 1, 0, true)
    block_wrkr_ids = map(ips->worker_ids[findin(worker_ips, ips)], block_dist)
    filestat = stat(f)
    block_sz = filestat.block_sz
    file_sz = filestat.size

    data = [(f, ((x-1)*block_sz+1):(min(file_sz,x*block_sz))) for x in 1:length(block_dist)]
    Blocks(f, filter, data, block_wrkr_ids)
end

