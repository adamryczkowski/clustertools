
RemoteServer<-R6::R6Class("RemoteServer",
  public = list(
    initialize=function(host_address, username=NULL,port=11001) {
#      browser()
      private$host_address_<-host_address

      if(is.null(username)) {
        username<-system('whoami', intern = TRUE)
      }
      default_if<-find_default_if()
      myip=system(paste0("ip addr show ", default_if, " | awk '$1 == \"inet\" {gsub(/\\/.*$/, \"\", $2); print $2}'"), intern=TRUE)


      private$cl_connection_ <- parallel::makeCluster(host_address, user=username, master=myip, port=port, homogeneous=FALSE)
      private$remote_tmp_dir_<-copy_scripts_to_server(private$cl_connection_)
      private$cl_pid_ <- parallel::clusterEvalQ(private$cl_connection_, Sys.getpid())


      run_background_task(private$cl_connection_,
                          pid=private$cl_pid_,
                          script_path = file.path(private$remote_tmp_dir_, 'peak_mem.sh'))

      private$job_history_<-JobHistory$new(stats_function=function() private$get_current_stats(flag_execute_on_aux = FALSE))
      private$cl_aux_connection_ <- parallel::makeCluster(host_address, user=username, master=myip, port=port, homogeneous=FALSE)
#      cl<-parallel::makeCluster(host_address, user=username, master=myip, port=port, homogeneous=FALSE)
      cl<-private$cl_aux_connection_

      private$capabilities_ <- BackgroundTask$new()
      private$capabilities_$run_task(c(get_cpu_capabilies(cl), remote_tmp_dir=private$remote_tmp_dir_))
    },

    finalize=function() {
      parallel::stopCluster(private$cl_connection_)
      parallel::stopCluster(private$cl_aux_connection_)
    },

    print=function() {
      rap<-paste0("Remote host ", self$host_name, " at ", self$host_address, " with ",
                  self$cpu_cores, " core CPU and ",
                  utils:::format.object_size(self$mem_size, "auto"), " RAM.\n",
                  "CPU speed measure: ", utils:::format.object_size(self$cpu_speed*1000000, "auto"), "/second\n",
                  "net_send_speed: ", utils:::format.object_size(self$net_send_speed*1000, "auto"), "/second\n",
                  "net_receive_speed: ", utils:::format.object_size(self$net_receive_speed*1000, "auto"), "/second\n\n"
      )
      cat(rap)
      current_load <- self$get_current_load()

      if(!is.null(current_load$command)) {
        rap<-paste0("\nCurrent task: ", current_load$command, "\n",
                    "Average CPU utilization: ", current_load$cpuload, "%\n",
                    "CPU time on task: ", lubridate::as.duration(current_load$cpu_time), "\n",
                    "Task current memory usage (delta): ",
                    utils:::format.object_size(current_load$mem_kb*1024, "auto"), " (",
                    utils:::format.object_size(current_load$mem_kb_delta*1024, "auto"), ")\n",
                    "Task peak memory usage (delta): ",
                    utils:::format.object_size(current_load$peak_mem_kb*1024, "auto"), " (",
                    utils:::format.object_size(current_load$peak_mem_kb_delta*1024, "auto"),")\n"
                    )
        cat(rap)
      }
      total_load <- self$get_current_load(flag_total_load = TRUE)
      rap<-paste0("\nTotal runnning statistics: \n",
                  "Average CPU utilization: ", total_load$cpuload, "%\n",
                  "CPU time spent: ", lubridate::as.duration(total_load$cpu_time), "\n",
                  if(is.null(current_load$command)) {
                    paste0("Current memory usage: ", utils:::format.object_size(total_load$mem_kb*1024, "auto"), "\n")
                  } else {""},
                  "Peak memory usage: ",
                  utils:::format.object_size(total_load$peak_mem_kb*1024, "auto"), "\n",
                  "Free memory: ", utils:::format.object_size(total_load$free_mem_kb*1024, "auto"), "\n",
                  "Total number of jobs finished / still in queue: ",
                  private$job_history_$get_finished_job_count(), " / ", private$job_history_$get_queued_job_count()
      )
      cat(rap)
    },

    get_count_statistics=function() {
      return(list(
        total = private$job_history_$get_job_count()-1, #We hide the initial task, because it is meaningless
        finished = private$job_history_$get_finished_job_count()-1,
        queued = private$job_history_$get_queued_job_count()
      ))
    },

    get_current_load=function(flag_total_load=FALSE) {
      if(flag_total_load) {
        running_job<-private$job_history_$get_first_job()
      } else {
        running_job<-private$job_history_$get_currently_running_job()
      }
      current_load<-get_current_load(cl=private$cl_aux_connection_, script_dir = private$remote_tmp_dir_, pid = private$cl_pid_)


      if(is.null(running_job)) {
        ans <- list(
          mem_kb=current_load$mem_kb,
          peak_mem_kb=current_load$peak_mem_kb,
          free_mem_kb=current_load$free_mem_kb)
      } else {
        last_stats <- running_job$get_job_stats_before()
        ans<-compute_load_between(load_before = last_stats, load_after = current_load)
      }
      return(ans)
    },

    get_total_load=function() {
      self$get_current_load(flag_total_load = TRUE)
    },

    get_last_job=function() {
      last_job_nr <- private$job_history_$get_running_job_nr()
      if(is.na(last_job_nr)) {
        last_job_nr <- private$job_history_$get_last_finished_job_nr()
        if(last_job_nr==1) {
          return(NULL)
        }
      }
      last_job <- private$job_history_$get_job_by_nr(last_job_nr)

      ans<-RemoteJob$new(job_entry=last_job, remote_server=self,
                         job_history=private$job_history_, job_nr=last_job_nr)
      return(ans)
    },

    is_busy=function() {
      return(private$job_history_$is_job_running())
    },

    get_job_by_name=function(jobname) {
      jobnrs<-private$job_history_$get_jobnr_by_name(jobname)
      if(length(jobnrs)==0) {
        return(list())
      }
      if(length(jobnrs)==1) {
        ans<-list(private$job_history_$get_job_by_nr(jobnrs))
      } else {
        ans<-list()
        for(i in seq(1,  length(jobnrs))){
          ans<-c(ans, list(private$job_history_$get_job_by_nr(jobnrs)))
        }
      }
      jobs<-lapply(seq(1, length(jobnrs)),
             function(i) RemoteJob$new(job_entry=ans[[i]], remote_server=self,
                                       job_history=private$job_history_, job_nr=jobnrs[[i]]))
      if(length(jobnrs)==1) {
        return(jobs[[1]])
      } else {
        return(jobs)
      }
    },

    .get_aux_connection=function() {private$cl_aux_connection_},
    .get_pid=function() {private$cl_pid_},
    .get_jobs=function(job_name=NULL) { private$job_history_},

    get_job_return_value=function(jobname, flag_remove_value=TRUE) {
      if(jobname=='') {
        stop("Jobname must be non-zero string")
      }
      job_nrs<-private$job_history_$get_jobnr_by_name(jobname)

      if(length(job_nrs)==0) {
        stop(paste0("Cannot find a job with name ", jobname))
      }
      if(length(job_nrs)==1) {
        job<-private$job_history_$get_job_by_nr(job_nrs)
        ans<-list(RemoteJob$new(job_entry=job, remote_server=self,
                           job_history=private$job_history_, job_nr=job_nrs))
      } else {
        ans<-list()
        for(job_nr in job_nrs) {
          job<-private$job_history_$get_job_by_nr(job_nr)
          ans<-c(ans, list(RemoteJob$new(job_entry=job, remote_server=self,
                                  job_history=private$job_history_, job_nr=job_nr)))
        }
      }
      jobnames<-sapply(ans, function(j) j$name )
      retvalue<-lapply(ans, function(j) {
        if(j$is_task_finished()) {
          j$get_return_value(flag_remove_value)
        } else{
          simpleError("Job has not finished")
        }})
      setNames(retvalue, jobnames)
      return(retvalue)
    },

    execute_job=function(jobname, expression, flag_wait=FALSE, timeout=0, flag_clear_memory=TRUE) {
      expr<-substitute(expression)
      command<-deparse(expr)
      ans<-eval(substitute(
        private$job_history_$run_task(jobname, {
          stats<-get_current_load(cl, remote_tmp_dir, pid)
          start_stats<-list(peak_mem_kb=stats$peak_mem_kb, cpu_time=stats$cpu_time, wall_time=stats$wall_time, mem_kb=stats$mem_kb)

          ans<-parallel::clusterEvalQ(cl = cl, expression)

          stats<-get_current_load(cl, remote_tmp_dir, pid)
          end_stats<-list(peak_mem_kb=stats$peak_mem_kb, cpu_time=stats$cpu_time, wall_time=stats$wall_time, mem_kb=stats$mem_kb,
                          free_mem_kb=stats$free_mem_kb
                          )
          return(list(start_stats=start_stats, ans=ans, end_stats=end_stats, pid=pid))
        }, command=command),
        list(cl=private$cl_connection_, remote_tmp_dir=private$remote_tmp_dir_, pid=private$cl_pid_,
             expression=expr, command=command)))
      job<-ans$job
      job_nr<-ans$jobnr

      if(flag_wait) {
        flag_is_running<-!(job$wait_until_finished(timeout=timeout))
      } else {
        flag_is_running<-TRUE
      }

      if(flag_is_running) {
        jobobj <- RemoteJob$new(job_entry=job, remote_server=self,
                                job_history=private$job_history_, job_nr=job_nr)
        return(jobobj)
      } else {
        ans <- job$get_return_value(flag_clear_memory=flag_clear_memory)
        return(ans)
      }
    },

    send_objects=function(named_list_of_objects, flag_wait=FALSE, job_name=NULL) {
      if(!'list' %in% class(named_list_of_objects)) {
        stop("named_list_of_objects must be a named list of objects to upload")
      }
      named_list_of_objects<-named_list_of_objects

      if(!flag_wait) {
        job<-self$create_job(job_name)

        job$run_task(
          send_big_objects(private$cl_connection_, objects = named_list_of_objects)
        )
        return(job)
      } else {
        send_big_objects(private$cl_connection_, objects = named_list_of_objects)
      }
    },

    send_file=function(local_path, remote_path, flag_wait=FALSE, flag_check_first=TRUE, job_name=NULL) {
      if(!flag_wait) {
        job<-self$create_job(job_name)

        job$run_task(
          send_file(private$cl_connection_, file_path = local_path, remote_path = remote_path,
                    flag_check_first=flag_check_first)
        )
        return(job)
      } else {
        send_file(private$cl_connection_, file_path = local_path, remote_path = remote_path,
                  flag_check_first=flag_check_first)
      }
    }

  ),

  active = list(
    host_address      = function() {private$host_address_},
    host_name      = function() {private$fill_capabilities(TRUE); private$capabilities_$host_name},
    cpu_cores         = function() {private$fill_capabilities(TRUE); private$capabilities_$cpu_cores},
    cpu_speed         = function() {private$fill_capabilities(TRUE); private$capabilities_$cpu_speed},
    mem_size          = function() {private$fill_capabilities(TRUE); private$capabilities_$mem_size},
    net_send_speed    = function() {private$fill_capabilities(TRUE); private$capabilities_$net_send_speed},
    net_receive_speed = function() {private$fill_capabilities(TRUE); private$capabilities_$net_receive_speed},
    remote_tmp_dir = function() {private$remote_tmp_dir_},
    cl_connection     = function() {private$cl_connection_},
    cl_aux_connection     = function() {private$cl_aux_connection_},
#    job = function() {private$job_},
    job_history = function() {private$job_history_}
  ),
  private = list(
    cl_aux_connection_=NA,
    cl_connection_=NA,
    cl_pid_=NA,
    capabilities_=NA,
    host_address_=NA,
    remote_tmp_dir_=NA,
    job_history_= NA,

    fill_capabilities=function(flag_wait=TRUE) {
      if('BackgroundTask' %in% class(private$capabilities_)){
        job<-private$capabilities_
        if(job$is_task_running() && !flag_wait )  {
          return(NULL)
        }
        capabilities<-job$get_task_return_value()
        ans<-list(
          cpu_cores =capabilities$cores,
          cpu_speed =capabilities$speed,
          mem_size =capabilities$mem_kb * 1024,
          net_send_speed =capabilities$net_send_speed,
          net_receive_speed =capabilities$net_receive_speed,
          host_name =capabilities$host_name
        )
        private$capabilities_<-ans
      }
    },

    get_last_executed_job_nr=function() {
      if(length(private$jobs_)>private$last_finished_job_) {
        for(i in seq(private$last_finished_job_, length(private$jobs_))) {
          job <- private$last_finished_job_[[i]]
          if (job$is_task_running()) {
            break
          }
          private$last_finished_job_ <- private$last_finished_job_ + 1
        }
      }
      return(private$last_finished_job_)
    },

    #Returns stats from the time, when the current job has been started
    get_last_stats=function() {
      last_job_idx<-get_last_executed_job_nr()
      if(last_job_idx<=length(private$counters_)+1) {
        last_stats<-private$counters_[[last_job_idx+1]]
      } else {
        last_stats<-NA
      }
      return(last_stats)
    },

    get_current_stats=function(flag_execute_on_aux=TRUE, flag_reset_peak_mem=FALSE) {
      if(flag_execute_on_aux) {
        cl <- private$cl_aux_connection_
      } else {
        cl <- private$cl_connection_
      }
      ans<-get_current_load(cl=cl, script_dir = private$remote_tmp_dir_, pid = private$cl_pid_)
      return(list(wall_time=ans$wall_time,
                  cpu_time=ans$cpu_time,
                  mem_kb=ans$mem_kb,
                  peak_mem_kb=ans$peak_mem_kb,
                  free_mem_kb=ans$free_mem_kb
      ))
      if(flag_reset_peak_mem) {
        file<-file.path(private$remote_tmp_dir_, 'reset_peak_mem.sh')
        eval(substitute(parallel::clusterEvalQ(system(file)),
                        list(file=file)))
      }
    }

  ),

  cloneable = FALSE,
  lock_class = TRUE
)

# srv1<-RemoteServer$new("rstudio")
# b1=srv1$job
# srv1$cpu_cores
# srv2<-RemoteServer$new('10.29.153.100')





