send_file<-function(cl, file_path, remote_path, flag_check_first=TRUE, flag_wait=TRUE) {
  if(!file.exists(file_path)) {
    stop(paste0("Cannot find ", file_path))
  }
  flag_may_skip_upload<-FALSE
  if(flag_check_first) {
    remote_hash<-eval(substitute(parallel::clusterEvalQ(cl, if(file.exists(remote_path)) {tools::md5sum(remote_path)} else{''}), list(remote_path=remote_path)))[[1]]
    if(remote_hash!='') {
      our_hash <-tools::md5sum(file_path)
      if(our_hash==remote_hash) {
        return()
      }
    }
  }

  if(!flag_may_skip_upload) {
    n<-file.size(file_path)
    a<-readBin(file(file_path, 'rb'), raw(), n=n)
    e<-new.env()
    assign('a', a, envir=e)
    parallel::clusterExport(cl, 'a', envir = e)
    os<-object.size(a)
    rm(a)
    rm(e)
    gc()
    remote_os<-parallel::clusterEvalQ(cl, object.size(a))[[1]]
    if(remote_os != os) {
      stop("Sent object has different size")
    }
    remote_path <- remote_path
    parallel::clusterExport(cl, varlist = 'remote_path', envir = environment())
    parallel::clusterEvalQ(cl, {f<-file(remote_path, 'wb'); writeBin(a, f, useBytes = TRUE); close(f);rm(list=c('a', 'remote_path'));1})
  }
  if(flag_wait) {
    private$job_$wait_for_task_finish()
  }
}

send_big_objects<-function(cl, objects, compress=NULL) {
  if(is.null(compress)) {
    if(as.numeric(object.size(objects)) < 100000) {
      compress<-'raw'
    }else {
      compress<-'bzip2'
    }
  }
  if(!'list' %in% class(objects)) {
    stop("objects must be a named list of objects to upload")
  }

  if(compress!='raw') {
    e<-new.env()
    raw<-memCompress(serialize(objects, connection=NULL), type=compress)
    assign('raw', raw, envir=e)
    parallel::clusterExport(cl, 'raw', envir = e)
    eval(substitute(parallel::clusterEvalQ(cl, {
      .tmp_imported_objects=unserialize(memDecompress(from = raw, type=compress))
      lapply(seq_along(.tmp_imported_objects),
             function(i) {
               assign(names(.tmp_imported_objects)[[i]], .tmp_imported_objects[[i]], .GlobalEnv)
             }
      )
      rm(.tmp_imported_objects)
      1}), list(compress=compress)))
    return(1)
  } else {
    parallel::clusterExport(cl, names(objects), envir = as.environment(objects))
    return(1)
  }
}

send_big_object<-function(cl, object, remote_name, compress=NULL) {
  if(is.null(compress)) {
    if(as.numeric(object.size(object)) < 100000) {
      compress<-'raw'
    }else {
      compress<-'bzip2'
    }
  }

  if(compress!='') {
    e<-new.env()
    raw<-memCompress(serialize(object, connection=NULL), type=compress)
    assign('raw', raw, envir=e)
    parallel::clusterExport(cl, 'raw', envir = e)
    eval(substitute(parallel::clusterEvalQ(cl, {assign(remote_name, unserialize(memDecompress(from = raw, type=compress)),.GlobalEnv);1}),
                    list(remote_name=remote_name, compress=compress)))
  } else {
    e<-new.env()
    assign('raw', object, envir=e)
    parallel::clusterExport(cl, remote_name, envir = e)
  }
}

receive_big_object<-function(cl, object_name, compress=NULL) {
  if(is.null(compress)) {
    obj<-tryCatch(
      eval(substitute(parallel::clusterEvalQ(cl, object.size(eval(parse(text=object_name)))),
                      list(object_name=parse(text=object_name), compress=compress))),

      error=function(e)e
    )
    if('error' %in% class(obj))
    {
      stop(paste0("Getting the remote object returned an error: ", obj$message))
    }
    if(as.numeric(obj) < 100000) {
      compress<-'raw'
    }else {
      compress<-'bzip2'
    }
  }

  if(compress!='') {
    ans<-eval(substitute(parallel::clusterEvalQ(cl, memCompress(serialize(eval(parse(text=object_name)), connection=NULL), type=compress)),
                         list(object_name=parse(text=object_name), compress=compress)))
    obj<-unserialize(memDecompress(from = ans[[1]], type=compress))
  } else {
    obj<-eval(substitute(parallel::clusterEvalQ(cl, object_name), list(object_name=parse(text=object_name))))
  }
  return(obj)
}

receive_big_objects<-function(cl, object_names, compress=NULL) {
  if(is.null(compress)) {
    obj<-tryCatch(
      unlist(eval(substitute(parallel::clusterEvalQ(cl, sum(as.numeric(lapply(object_names,
                                                           function(object_name) object.size(eval(parse(text=object_name))))))),
                      list(object_names=object_names)))),

      error=function(e)e
    )
    if('error' %in% class(obj))
    {
      stop(paste0("Getting the remote object returned an error: ", obj$message))
    }
    if(as.numeric(obj) < 100000) {
      compress<-'raw'
    }else {
      compress<-'bzip2'
    }
  }

  if(compress!='raw') {
    ans<-eval(substitute(parallel::clusterEvalQ(cl, {
        e<-new.env()
        for(object_name in object_names) {
          e$object_name <- eval(parse(text=object_name))
        }
        memCompress(serialize(e, connection=NULL), type=compress)
      }), list(object_names=object_names, compress=compress)))
    objs<-unserialize(memDecompress(from = ans[[1]], type=compress))
  } else {
    objs<-eval(substitute(parallel::clusterEvalQ(cl, {
        e<-new.env()
        for(object_name in object_names) {
          assign(object_name, eval(parse(text=object_name)), envir=e)
        }
        e
      }), list(object_names=object_names)))
  }
  return(objs)
}

execute_remote<-function(cl, expr) {
  expr <- substitute(expression)

  job <- BackgroundTask$new()

  job$run_task({
    stats<-get_current_load(private$cl_connection, private$remote_tmp_dir_, private$cl_id_)
    start_stats<-list(peak_mem_kb=stats$peak_mem_kb, cpu_time=stats$cpu_time, wall_time=stats$wall_time)

    ans<-parallel::clusterEvalQ(cl = private$cl_connection, expression)

    stats<-get_current_load(private$cl_connection, private$remote_tmp_dir_, private$cl_id_)
    end_stats<-list(peak_mem_kb=stats$peak_mem_kb, cpu_time=stats$cpu_time, wall_time=stats$wall_time)
    return(list(start_stats=start_stats, ans=ans, end_stats=end_stats))
  }
  )
  return(job)
}

# srv_loc<-RemoteServer$new('localhost')
# debugonce(srv_loc$print)
# debugonce(srv_loc$pri)
# print(srv_loc)
