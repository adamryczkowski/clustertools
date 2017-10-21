
RemoteServer<-R6::R6Class("RemoteServer",
  public = list(
    initialize=function(host_address, username=NULL,port=11001, network_interface=NULL) {
      #browser()
      can_connect<-can_connect_to_host(host_address)
      if(can_connect!="") {
        stop(paste0(can_connect))
      }
      private$host_address_<-host_address

      private$mutex_prev_ <- get_mutex()#synchronicity::boost.mutex(synchronicity::uuid())
      private$mutex_main_ <- get_mutex()

      if(is.null(username)) {
        username<-system('whoami', intern = TRUE)
      }
      if(is.null(network_interface)) {
         network_interface<-find_default_if(host_address)
      }
      myif<-find_default_if(target_ip = host_address)
      myip<-ifaddr(myif[[1]])[[1]]
        #system(paste0("ip addr show ", network_interface, " | awk '$1 == \"inet\" {gsub(/\\/.*$/, \"\", $2); print $2}'"), intern=TRUE)


      private$cl_connection_ <- parallel::makeCluster(host_address, user=username, master=myip, port=port, homogeneous=FALSE)


      private$remote_tmp_dir_<-copy_scripts_to_server(private$cl_connection_)
      private$cl_pid_ <- MyClusterEval(private$cl_connection_, Sys.getpid())
#      private$cl_pid_ <- parallel::clusterEvalQ(private$cl_connection_, Sys.getpid())

      run_background_task(private$cl_connection_,
                          pid=private$cl_pid_,
                          script_path = file.path(private$remote_tmp_dir_, 'peak_mem.sh'))

      mydir<-system.file('scripts', package='clustertools')
      all_script_names<-file.path(private$remote_tmp_dir_, list.files(mydir, pattern='\\.sh$'))

      do_script_exist<-eval(substitute(
        MyClusterEval(private$cl_connection_, file.exists(scripts)),
#        parallel::clusterEvalQ(private$cl_connection_, file.exists(scripts)),
        list(scripts=all_script_names)))[[1]]
      if(!all(do_script_exist)) {
        browser()
        stop("Copying scripts to remote host failed")
      }

      private$cl_aux_connection_ <- parallel::makeCluster(host_address, user=username, master=myip, port=port, homogeneous=FALSE)
      private$job_history_<-JobHistory$new(stats_function=function() private$get_current_stats(flag_execute_on_aux = TRUE), server=self)
#      cl<-parallel::makeCluster(host_address, user=username, master=myip, port=port, homogeneous=FALSE)
      cl<-private$cl_aux_connection_

      private$set_capabilities()
      self$run_benchmark()

#      private$capabilities_ <- BackgroundTask$new()
#      private$capabilities_$run_task(c(get_cpu_capabilies(cl), remote_tmp_dir=private$remote_tmp_dir_))

#      private$fill_capabilities()
#      Sys.sleep(0.2)

      # for(i in 1:30) {
      #   get_current_load(private$cl_connection_, script_dir = private$remote_tmp_dir_, pid = private$cl_pid_)
      # }

    },

    finalize=function() {
      tryCatch(parallel::stopCluster(private$cl_aux_connection_),
               error=function(e)e)
      tryCatch(parallel::stopCluster(private$cl_connection_),
               error=function(e)e)
#      if(!is.atomic(private$mutex_main_)) {
#        synchronicity::unlock(private$mutex_main_)
#      }
#      if(!is.atomic(private$mutex_prev_)) {
#        synchronicity::unlock(private$mutex_prev_)
#      }
    },

    print=function() {
      if(is.na(self$cpu_cores)) {
        rap<-paste0("Remote host ", self$host_address, ". Benchmarks and specifications not available yet.\n\n"
        )
      } else {
        rap<-paste0("Remote host ", self$host_name, " at ", self$host_address, " with ",
                    self$cpu_cores, " core CPU and ",
                    utils:::format.object_size(self$mem_size, "auto"), " RAM.\n",
                    if(self$cpu_speed2=='') {
                      paste0("CPU speed measure: ", utils:::format.object_size(self$cpu_speed*1000000, "auto"), "/second\n")
                    } else {
                      paste0("CPU speed: ", gsub('.{1}$', '', utils:::format.object_size(2000/self$cpu_speed2, "auto")), " primes/second\n")
                    },
                    "net_send_speed: ", utils:::format.object_size(self$net_send_speed*1000, "auto"), "/second\n",
                    "net_receive_speed: ", utils:::format.object_size(self$net_receive_speed*1000, "auto"), "/second\n",
                    "ping_time: ", round(self$ping_time*1000), " ms\n\n"
        )
      }
      cat(rap)
      current_load <- self$get_current_load()

      if(!is.null(current_load$command)) {
        rap<-paste0("\nCurrent task: ", current_load$name, "\n",
                    "Average CPU utilization: ", round(current_load$cpuload*100, 2), "%\n",
                    "CPU time on task: ", lubridate::as.duration(current_load$cpu_time), "\n",
                    "Task current memory usage (delta): ",
                    utils:::format.object_size(current_load$mem_kb*1024, "auto"), " (",
                    utils:::format.object_size(current_load$mem_kb_delta*1024, "auto"), ")\n",
                    "Task peak memory usage (delta): ",
                    utils:::format.object_size(current_load$peak_mem_kb*1024, "auto"), " (",
                    utils:::format.object_size(current_load$peak_mem_kb_delta*1024, "auto"),")\n",
                    "Code:\n", current_load$command, "\n"
                    )
        cat(rap)
      }
      total_load <- self$get_current_load(flag_total_load = TRUE)
      rap<-paste0("\nTotal runnning statistics: \n",
                  "Average CPU utilization: ", round(total_load$cpuload*100, 2), "%\n",
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
      private$job_history_$is_job_running()
      return(list(
        total = private$job_history_$get_job_count()-1, #We hide the initial task, because it is meaningless
        finished = private$job_history_$get_finished_job_count()-1,
        queued = private$job_history_$get_queued_job_count()
      ))
    },

    #Runs benchmark. When completes, the results from the benchmark will overwrite the old ones
    get_current_load=function(flag_total_load=FALSE) {
#      browser()
      if(flag_total_load) {
        running_job<-private$job_history_$get_first_job()
      } else {
        running_job<-private$job_history_$get_currently_running_job()
      }
      current_load<-get_current_load(cl=private$cl_aux_connection_, script_dir = private$remote_tmp_dir_, pid = private$cl_pid_)
      if(!'list' %in% class(current_load)) {
        browser()
        current_load<-get_current_load(cl=private$cl_aux_connection_, script_dir = private$remote_tmp_dir_, pid = private$cl_pid_)
      }


      if(is.null(running_job)) {
        ans <- list(
          mem_kb=current_load$mem_kb,
          peak_mem_kb=current_load$peak_mem_kb,
          free_mem_kb=current_load$free_mem_kb)
      } else {
        last_stats <- running_job$get_job_stats_before()
        ans<-c(compute_load_between(load_before = last_stats, load_after = current_load),
               command=running_job$command,
               name=running_job$name)
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

    get_job_by_name=function(job_name) {
      jobnrs<-private$job_history_$get_jobnr_by_name(job_name)
      if(length(jobnrs)==0) {
        return(list())
      }
      if(length(jobnrs)==1) {
        ans<-list(private$job_history_$get_job_by_nr(jobnrs))
      } else {
        ans<-list()
        for(i in jobnrs){
          ans<-c(ans, list(private$job_history_$get_job_by_nr(i)))
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

    get_job_by_nr=function(job_nr) {
      ans<-private$job_history_$get_job_by_nr(jobnr=job_nr+1) #We hide the initial job
      job<-RemoteJob$new(job_entry=ans, remote_server=self,
                          job_history=private$job_history_, job_nr=job_nr+1)
      return(job)
    },
    .get_aux_connection=function() {private$cl_aux_connection_},
    .get_main_connection=function() {private$cl_connection_},
    .get_pid=function() {private$cl_pid_},
    .get_jobs=function(job_name=NULL) { private$job_history_},

    get_job_return_value=function(job_name, flag_remove_value=TRUE) {
      if(job_name=='') {
        stop("job_name must be non-zero string")
      }
      job_nrs<-private$job_history_$get_jobnr_by_name(job_name)

      if(length(job_nrs)==0) {
        stop(paste0("Cannot find a job with name ", job_name))
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

    wait_for_all_tasks=function(timeout=0) {
      n<-private$job_history_$get_job_count()
      j<-private$job_history_$get_job_by_nr(n)
      j$wait_until_finished(timeout)
    },

    run_benchmark=function() {
      command<-paste0("<benchmarking task>")

      env<-new.env()
      env$cl<-private$cl_connection_
      tag<-"benchmark"
      attr(tag, 'callback')<-private$set_capabilities
      ans<-private$execute_wait_(quote({
        get_cpu_capabilies(cl)
      }), env=env, command=command, job_name="", timeout=-1, flag_clear_memory=TRUE, tag=tag)
      return(NULL)
    },

    #If timeout<0 then function will never wait.
    #Default value: -1 when job_name is given, and 0 (wait indefinitely) when job_name is missing.
    execute_job=function(expression_, job_name="", timeout=NULL, flag_clear_memory=TRUE) {
      expr<-substitute(expression_)
      env<-new.env()
      env$expr_execute_job_<-expr
      env$cl<-private$cl_connection_
      command<-deparse(expr)
      if(is.null(timeout)) {
        if(job_name!="") {
          timeout=-1
        } else {
          timeout=0
        }
      }

      ans<-private$execute_wait_(substitute(parallel::clusterEvalQ(cl = cl, expr), list(expr=expr)),
                                 env=env, command, job_name=job_name, timeout=timeout,
                                 flag_clear_memory=flag_clear_memory)
      return(ans)
    },

    send_objects=function(named_list_of_objects, flag_wait=FALSE, job_name=NULL, timeout=0, compress='auto') {
      if(!'list' %in% class(named_list_of_objects)) {
        stop("named_list_of_objects must be a named list of objects to upload")
      }
      if(compress=='auto') {
        compress<-NULL
      }

      command<-paste0("<sending ", length(named_list_of_objects), " object", if(length(named_list_of_objects)>1) "s", " (",
                      utils:::format.object_size(object.size(named_list_of_objects),"auto"), ")>")

      if(is.null(timeout)) {
        if(job_name!="") {
          timeout=-1
        } else {
          timeout=0
        }
      }

      env<-new.env()
      env$named_list_of_objects<-named_list_of_objects
      env$compress<-compress
      env$cl<-private$cl_connection_

      ans<-private$execute_wait_(quote({

          send_big_objects(cl, objects = named_list_of_objects, compress = compress)
          paste0(if(length(named_list_of_objects)==1) {
            "1 object sent."
          } else {
            paste0(length(named_list_of_objects), " objects sent.")
          })
        }),
        env=env,
        command=command, job_name=job_name, timeout=timeout, flag_clear_memory=FALSE)

      return(ans)
    },

    receive_objects=function(object_names, flag_wait=FALSE, job_name=NULL, compress='auto', timeout=0, flag_clear_memory=TRUE) {
      if(compress=='auto') {
        compress<-NULL
      }
      if(!'character' %in% class(object_names)) {
        stop("object_names must be a vector of names of variables to download")
      }

      command<-paste0("<receiving ", length(object_names), " object", if(length(object_names)>1) "s", " (",
                      paste0(object_names, collapse = ", "), ")>")

      env<-new.env()
      env$cl<-private$cl_connection_
      env$object_names<-object_names
      env$compress<-compress

      ans<-private$execute_wait_(quote(
        receive_big_objects(cl, object_names = object_names, compress=compress)), env=new.env(),
        command=command, job_name=job_name, timeout=timeout, flag_clear_memory=flag_clear_memory)
      return(ans)
    },

    send_file=function(local_path, remote_path, flag_wait=FALSE, flag_check_first=TRUE, timeout=0, job_name=NULL) {
      if(!'character' %in% class(local_path)) {
        stop("local_path must be a filename")
      }
      if(!'character' %in% class(remote_path)) {
        stop("remote_path must be a filename")
      }

      command<-paste0("<sending file ", pathcat::make.path.relative(getwd(), local_path), " (",
                      utils:::format.object_size(file.size(local_path),"auto"), ")>")

      env<-new.env()
      env$cl<-private$cl_connection_
      env$file_path<-local_path
      env$remote_path<-remote_path
      env$flag_check_first<-flag_check_first
      ans<-private$execute_wait_(quote(send_file(cl, file_path = local_path, remote_path = remote_path,
                                                 flag_check_first=flag_check_first)), env=env,
                                 command=command, job_name=job_name, timeout=timeout, flag_clear_memory=FALSE)
      return(ans)
    }

  ),

  active = list(
    host_address      = function() {private$host_address_},
    host_name      = function() {private$fill_capabilities(TRUE); private$capabilities_$host_name},
    cpu_cores         = function() {private$fill_capabilities(TRUE); private$capabilities_$cpu_cores},
    cpu_speed         = function() {private$fill_capabilities(TRUE); private$capabilities_$cpu_speed},
    cpu_speed2         = function() {private$fill_capabilities(TRUE); private$capabilities_$cpu_speed2},
    mem_size          = function() {private$fill_capabilities(TRUE); private$capabilities_$mem_size},
    net_send_speed    = function() {private$fill_capabilities(TRUE); private$capabilities_$net_send_speed},
    net_receive_speed = function() {private$fill_capabilities(TRUE); private$capabilities_$net_receive_speed},
    ping_time = function() {private$fill_capabilities(TRUE); private$capabilities_$ping_time},
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
    mutex_main_ = NA,#This mutex locks critical queue managing code.
    mutex_prev_ = NA,#synchronicity::boost.mutex(synchronicity::uuid()), #Place for mutexes that serializes execution of remote threads. Each mutex is held by the currently executing thread, and released upon exit.
                                                #Each new thread gets a new copy of the mutex. Executing threads form a single linked list, when the chain is the mutex.
                                                #When there is no jobs, this mutex is NULL.
                                                #When there are jobs, this mutex is a mutex that will get released when the last job finishes

    fill_capabilities=function(flag_wait=TRUE) {
      job<-private$job_history_$get_job_by_nr(2)
      job$is_task_finished() #If the task is finished, this will probe the task and call the callback which will update the capabilities.
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
        eval(substitute(parallel::clusterEvalQ(cl=cl, system(file)),
                        list(file=file, cl=cl)))
      }
    },
    #expr_after will be executed locally and asynchronously (will not block spawning another job).
    execute_=function(expr, env, command, job_name="", tag="normal") {

      m_prev_mutex<-private$mutex_prev_
      m_main_mutex<-private$mutex_main_
      m_next_mutex<-get_mutex()
      m_job_mutex<-get_mutex()
      lock_mutex(m_next_mutex)#We will not let the job start until we finish management

      lock_mutex(private$mutex_main_)#Starting exlcusive mode. The remote thread will unlock the mutex when it finished
      #first step of the setting up process
      env$cl<-private$cl_connection_
      envloc<-new.env(parent = env)
      envloc$expr<-expr
      envloc$cl=private$cl_connection_
      envloc$cl2=private$cl_aux_connection_
      envloc$remote_tmp_dir=private$remote_tmp_dir_
      envloc$pid=private$cl_pid_
      envloc$m_main_descr=synchronicity::describe(m_main_mutex)
      envloc$m_previous_descr=synchronicity::describe(m_prev_mutex)
      envloc$m_me_descr=synchronicity::describe(m_job_mutex)
      envloc$m_next_descr=synchronicity::describe(m_next_mutex)
      envloc$env=env
      envloc$tag=tag

#      env$_expr_RemoteServer_<-expr
 #     private$job_ <- eval(quote(parallel::mcparallel(_expr_BackgroundTask_)), envir = env)

      ans<-private$job_history_$run_task_(job_name, quote({
          m_main<-synchronicity::attach.mutex(m_main_descr) #Main is locked and the main thread
          #waits for us to signal, that we are done with the setup jobs
          m_previous<-synchronicity::attach.mutex(m_previous_descr) #This mutex will be freed when
          #the previous jobs finish its remote part
          m_next<-synchronicity::attach.mutex(m_next_descr) #We will free this mutex at the end of our remote work,
          #so the next task in the queue can take the server
          m_me<-synchronicity::attach.mutex(m_me_descr) #This mutex will be locked for the whole duration of our processing,
          #including local chores.

          lock_mutex(m_me) #Start being busy for the job object
          unlock_mutex(m_main) #Signalling the master thread we are done with init. The Init thread continues with
          #getting ready to accept end of our work
          lock_mutex(m_next) #Here we wait until the master is ready to accept the fact that we may have finished our work.


          lock_mutex(m_previous) #Only now waiting for the previous task to finish
          lock_mutex(m_main) #We lock main when we are about to start executing in order to avoid race condtion,
          unlock_mutex(m_previous) #Remove now unnecesary mutex
          unlock_mutex(m_main) #We have started
          rm(m_previous)
          #when user wants to abort our thread exactly now
          if (synchronicity::lock(m_me, block=FALSE)==FALSE) {
            #We are still busy, so we have not been canceled

            stats<-get_current_load(cl, remote_tmp_dir, pid)
            start_stats<-list(peak_mem_kb=stats$peak_mem_kb, cpu_time=stats$cpu_time, wall_time=stats$wall_time, mem_kb=stats$mem_kb)

            ans<-tryCatch({
              eval(expr, env)
            }, error=function(e) e)

            stats<-get_current_load(cl, remote_tmp_dir, pid)

            end_stats<-list(peak_mem_kb=stats$peak_mem_kb, cpu_time=stats$cpu_time, wall_time=stats$wall_time, mem_kb=stats$mem_kb,
                            free_mem_kb=stats$free_mem_kb
            )
          } else {
            #We are being canceled, so no execution
            start_stats="cancelled"
            end_stats="cancelled"
            ans<-"cancelled"
          }

          unlock_mutex(m_me)
          unlock_mutex(m_next)
          list(start_stats=start_stats, ans=ans, end_stats=end_stats, pid=pid, tag=tag)
        }), env=envloc, command=command, mutex=m_job_mutex)
        # list(cl=private$cl_connection_, cl2=private$cl_aux_connection_, remote_tmp_dir=private$remote_tmp_dir_, pid=private$cl_pid_,
        #      m_main_descr=synchronicity::describe(m_main_mutex),
        #      env=env,
        #      m_previous_descr=synchronicity::describe(m_prev_mutex),
        #      m_me_descr=synchronicity::describe(m_job_mutex),
        #      m_next_descr=synchronicity::describe(m_next_mutex))),
        #envir=env)

      lock_mutex(private$mutex_main_)#Here we will wait for the remote job to unlock this mutex for us. If for some reason the remote
      #thread will not spawn, no one will unlock the mutex and we will be stuck forever

      #Now the remote thread waits for us to start waiting for the remote job
      private$mutex_prev_ <- m_next_mutex
      unlock_mutex(m_next_mutex)#Ok. Let it run remotely. Now two threads run in parallel (and the remote thread will spawn a process in the cluster)
      unlock_mutex(m_main_mutex)


      job<-ans$job
      job_nr<-ans$jobnr

      return(list(job=job, jobnr=job_nr))
    },
    execute_wait_=function(expr, envir, command, job_name="", timeout, flag_clear_memory=FALSE, tag="normal") {
      ans<-private$execute_(expr, envir, command, job_name=job_name, tag=tag)

      job<-ans$job
      job_nr<-ans$jobnr

      if(timeout>=0) {
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
        if('simpleError' %in% class(ans)){
          stop(paste0("The node ", private$host_address_, " returned an error:\n«",
                      stringr::str_replace(ans$message, pattern = stringr::fixed("one node produced an error: "),replacement = ""),
                      "»\nwhen processing the command:\n   ", command))
        }
        return(ans)
      }
    },
  set_capabilities=function(capabilities=NULL) {
    if(is.null(capabilities)) {
      private$capabilities_<-list(
        cpu_cores =NA,
        cpu_speed =NA,
        cpu_speed2 =NA,
        mem_size =NA,
        net_send_speed =NA,
        net_receive_speed =NA,
        host_name =private$host_address_,
        ping_time =NA,
        wall_time = Sys.time()
      )
    } else{
      private$capabilities_<-list(
        cpu_cores =capabilities$cores,
        cpu_speed =capabilities$speed,
        cpu_speed2 =capabilities$speed2,
        mem_size =capabilities$mem_kb * 1024,
        net_send_speed =capabilities$net_send_speed,
        net_receive_speed =capabilities$net_receive_speed,
        host_name =capabilities$host_name,
        ping_time =capabilities$ping_time,
        wall_time = Sys.time()
      )
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


