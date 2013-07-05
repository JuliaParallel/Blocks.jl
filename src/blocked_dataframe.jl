
function _mf_file_dataframe(m::Function, x::Tuple)
    r = x[2]
    bio = BlockIO(open(x[1]), r, '\n')
    hashdr = (1==r.start)  # handle other cases like no header etc
    tbl = readtable(bio, header=hashdr)
    m(tbl)
end

pmap{T<:File,D<:DataFrame}(m, bf::Blocks{T,D}...) = pmap_base(x->_mf_file_dataframe(m,x), bf...)

