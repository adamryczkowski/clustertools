
#This is high-level interface to the scheduled remote job
RemoteJob<-R6::R6Class("RemoteJob",
  public = list(
    initialize=function(job_entry, remote_server, job_history, job_nr) {
      private$job_entry_ <- job_entry
      private$remote_server_ <- remote_server
      private$job_history_ <- job_history
      private$job_nr_ <- job_nr
    },

    is_finished = function() {
      return(private$job_entry_$is_task_finished())
    },

    is_running = function() {
      running_job_nr <- private$job_history_$get_running_job_nr()
      if(is.na(running_job_nr)) {
        return(FALSE)
      }
      return(running_job_nr == private$job_nr_)
    },

    is_scheduled = function() {
      running_job_nr <- private$job_history_$get_running_job_nr()
      if(is.na(running_job_nr)) {
        return(FALSE)
      }
      return(running_job_nr < private$job_nr_)
    },

    #Gets all the jobs that are sitting in front of us in the queue
    list_jobs_scheduled_after=function() {
      running_job_nr <- private$job_history_$get_running_job_nr()
      if(is.na(running_job_nr)) {
        stop("No job is running at the moment")
      }
      if(running_job_nr >= private$job_nr_) {
        stop("The job already run or is currently running")
      }
      ans<-list()
      for(jobnr in seq(running_job_nr, private$job_nr_-1)) {
        job <- private$job_history_$get_job_by_nr(jobnr)
        jobobj <- RemoteJob$new(job_entry = job, remote_server=private$remote_server_, job_history=private$job_history, job_nr=jobnr)
        ans<-c(ans, setNames(list(jobobj), job$name))
      }
      return(ans)
    },

    #Gets all the jobs that are sitting in front of us in the queue
    count_jobs_scheduled_after=function() {
      running_job_nr <- private$job_history_$get_running_job_nr()
      if(is.na(running_job_nr)) {
        return(0)
      }
      if(running_job_nr >= private$job_nr_) {
        return(0)
      }
      return(private$job_nr_-1 - running_job_nr)
    },

    peek_return_value = function(flag_wait_until_finished=FALSE, timeout=0) {
      if(flag_wait_until_finished) {
        flag_ready<-private$job_entry_$wait_until_finished(timeout=timeout)
        if(!flag_ready) {
          return(simpleError("Job is still executing"))
        }
      }
      ans<-private$job_entry_$get_return_value(flag_clear_memory=FALSE)
      if('simpleError' %in% class(ans)){
        if(stringr::str_detect(ans$message, pattern = stringr::fixed("one node produced an error: "))) {
          stop(paste0("The node ", private$host_address_, " returned an error:\n«",
                      stringr::str_replace(ans$message, pattern = stringr::fixed("one node produced an error: "),replacement = ""),
                      "»\nwhen processing the command:\n   ", private$job_entry_$command))
        } else {
          stop(ans$message)
        }
      }
      return(ans)
    },

    pop_return_value = function(flag_wait_until_finished=FALSE, timeout=0) {
      if(flag_wait_until_finished) {
        flag_ready<-private$job_entry_$wait_for_task_finish(timeout=timeout)
        if(!flag_ready) {
          return(simpleError("Job is still executing"))
        }
      }
      ans<-private$job_entry_$get_return_value(flag_clear_memory=TRUE)
      if('simpleError' %in% class(ans)) {
        stop(ans$message)
      }
      return(ans)
    },

    get_current_statistics=function() {
      if(self$is_scheduled()) {
        ans=list(state='scheduled',
                 name=private$job_entry_$name,
                 queue_length=self$count_jobs_scheduled_after(),
                 command=private$job_entry_$command
                 )
      } else if (self$is_running()) {
        stats<-private$remote_server_$get_current_load()
        ans<-c(list(state='running',
                    name=private$job_entry_$name,
                    command=private$job_entry_$command),
               stats)
      } else if (self$is_finished()) {
        ans<-c(list(state='finished',
                    name=private$job_entry_$name,
                    command=private$job_entry_$command),
                    compute_load_between(private$job_entry_$get_job_stats_before(),
                                         private$job_entry_$get_job_stats_after()))
      }
      return(ans)
    },

    print=function(flag_include_command=FALSE) {
#      browser()
      stats<-self$get_current_statistics()
      rap <- paste0('Task ', stats$name,
                    if(flag_include_command) {
                      paste0('\n\n', stats$command,'\n\n')
                    } else {
                      '\n'
                    },
                    'State: ', stats$state,'\n')

      if(stats$state == 'scheduled') {
        rap <- paste0(rap, "Number of tasks before: ", stats$queue_length)
      } else if (stats$state=='running') {
        rap <- paste0(rap,
                      "Average CPU utilization: ", round(stats$cpuload*100, 2), "%\n",
                      "CPU time on task: ", lubridate::as.duration(round(stats$cpu_time,2)), "\n",
                      "Wall time on task: ", lubridate::as.duration(round(stats$wall_time,2)), "\n",
                      "Task current memory usage (delta): ",
                      utils:::format.object_size(stats$mem_kb*1024, "auto"), " (",
                      utils:::format.object_size(stats$mem_kb_delta*1024, "auto"), ")\n",
                      "Task peak memory usage (delta): ",
                      utils:::format.object_size(stats$peak_mem_kb*1024, "auto"), " (",
                      utils:::format.object_size(stats$peak_mem_kb_delta*1024, "auto"),")\n"
        )
      } else if (stats$state =='finished') {
        rap <- paste0(rap,
                      "Average CPU utilization: ", round(stats$cpuload*100, 2), "%\n",
                      "CPU time on task: ", lubridate::as.duration(round(stats$cpu_time,2)), "\n",
                      "Wall time on task: ", lubridate::as.duration(round(stats$wall_time,2)), "\n",
                      "Task peak memory usage (delta): ",
                      utils:::format.object_size(stats$peak_mem_kb*1024, "auto"), " (",
                      utils:::format.object_size(stats$peak_mem_kb_delta*1024, "auto"),")\n"
        )
      }
      cat(rap)
    },
    get_parallelJob=function() {
      return(private$job_entry_$job$job)
    }
  ),
  active = list(
    command = function() {return(private$job_entry_$command)}
  ),
  private =list(
    job_entry_=NA,
    remote_server_=NA,
    job_history_=NA,
    job_nr_=NA
  ),

  cloneable = FALSE,
  lock_class = TRUE
)
