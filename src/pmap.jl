function block_pmap(f, lsts...; err_retry=true, err_stop=false, fetch_results=true)
    len = length(lsts)
    np = nworkers()
    retrycond = Condition()

    results = Dict{Int,Any}()
    function setresult(idx,v)
        results[idx] = v
        notify(retrycond)
    end

    retryqueue = Any[]
    function retry(idx,v,ex)
        push!(retryqueue, (idx,v,ex))
        notify(retrycond)
    end

    task_in_err = false
    is_task_in_error() = task_in_err
    set_task_in_error() = (task_in_err = true)

    nextidx = 0
    getnextidx() = (nextidx += 1)
    getcurridx() = nextidx

    states = [start(lsts[idx]) for idx in 1:len]
    function producer()
        while true
            if !isempty(retryqueue)
                produce(shift!(retryqueue)[1:2])
            elseif all([!done(lsts[idx],states[idx]) for idx in 1:len])
                nxts = [next(lsts[idx],states[idx]) for idx in 1:len]
                map(idx->states[idx]=nxts[idx][2], 1:len)
                nxtvals = [x[1] for x in nxts]
                produce((getnextidx(), nxtvals))
            elseif (length(results) == getcurridx()) || (is_task_in_error() && err_stop)
                break
            else
                wait(retrycond)
            end
        end
    end

    pt = Task(producer)
    @sync begin
        wrkrs = workers()
        for p=1:np
            wpid = wrkrs[p]
            @async begin
                for (idx,nxtvals) in pt
                    try
                        if fetch_results
                            result = remotecall_fetch(f, wpid, nxtvals...)
                            isa(result, Exception) ? rethrow(result) : setresult(idx, result)
                        else
                            result_ref = remotecall_wait(f, wpid, nxtvals...)
                            if err_stop || err_retry        # avoid fetching the type if we won't use it
                                result_type = remotecall_fetch((x)->typeof(fetch(x)), wpid, result_ref)
                                isa(result_type, Exception) ? rethrow(fetch(result_ref)) : setresult(idx, result_ref)
                            else
                                setresult(idx, result_ref)
                            end
                        end
                    catch ex
                        err_retry ? retry(idx,nxtvals,ex) : setresult(idx, ex)
                        set_task_in_error()
                        break # remove this worker from accepting any more tasks
                    end
                end
            end
        end
    end

    !istaskdone(pt) && throwto(pt, InterruptException())
    for failure in retryqueue
        results[failure[1]] = failure[3]
    end
    [results[x] for x in 1:nextidx]
end

##
# generic map on any iterator
#
#function map(f::Union{Function,DataType}, iters...)
#    result = {}
#    len = length(iters)
#    states = [start(iters[idx]) for idx in 1:len]
#
#    while all([!done(iters[idx],states[idx]) for idx in 1:len])
#        nxts = [next(iters[idx],states[idx]) for idx in 1:len]
#        map(idx->states[idx]=nxts[idx][2], 1:len)
#        nxtvals = [x[1] for x in nxts]
#        push!(result, f(nxtvals...))
#    end
#    [result...]
#end
#
