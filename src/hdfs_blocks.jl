
function Blocks{T<:IO}(f::HdfsFile, outtype::T)
    worker_ids = workers()
    worker_ips = map(x->getaddrinfo(isa(x, LocalProcess)?getipaddr():x.host), map(x->Base.worker_from_id(x), worker_ids))

    block_dist = hdfs_blocks(f, 1, filesize(f), true)
    block_wrkr_ids = map(ips->worker_ids[findin(worker_ips, ips)], block_dist)
    block_sz = stat(f).block_sz

    data = [(f, ((x-1)*block_sz+1):(x*block_sz)) for x in 1:length(block_dist)]
    Blocks(f, outtype, data, block_wrkr_ids)
end

#pmap{T<:HdfsFile,D<:DataFrame}(m, bf::Blocks{T,D}...; kwargs...) = pmap_base(x->_mf_file_dataframe(m,x;kwargs...), bf...)

