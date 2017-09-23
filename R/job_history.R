JobHistory<-R6::R6Class("JobHistory",
  public = list(
    initialize=function(stats_function) {
#      browser()

      private$get_stats_function_ <- stats_function
      private$jobs_=list()
      initjob <- private$create_job('.init','', flag_init_job=TRUE)
      private$last_finished_job_=1
    },

    #expression requires something that evaluates to list of 3 elements:
    #start_stats, ans, end_stats
    run_task=function(job_name, expression, command) {
      #First it gathers current statistics
      expr <- substitute(expression)

      job <- private$create_job(job_name, command = command)

      job$job$run_task(expr)
      return(list(job=job, jobnr=length(private$jobs_)))
    },

    is_job_running=function() {
      return(!is.na(self$get_running_job_nr()))
    },

    get_running_job_nr=function() {
      last_nr <- self$get_last_finished_job_nr()
      if(last_nr == length(private$jobs_)) {
        return(NA)
      } else {
        return(last_nr+1)
      }
    },

    get_currently_running_job=function() {
      jobnr<-self$get_running_job_nr()
      if(is.na(jobnr)) {
        return(NULL)
      }
      job<-private$jobs[[jobnr]]
      return(job)
    },

    get_last_finished_job_nr=function() {
      if(length(private$jobs_)>private$last_finished_job_) {
        for(i in seq(private$last_finished_job_, length(private$jobs_))) {
          job <- private$jobs_[[i]]
          if (!job$is_task_finished()) {
            return(private$last_finished_job_) #Return last job, not the current, because it is still running
          }
          private$last_finished_job_ <- i
        }
      }
      return(private$last_finished_job_)
    },

    get_last_finished_job=function() {
      jobnr<-self$get_last_finished_job_nr()
      if(is.na(jobnr)) {
        return(NULL)
      }
      job<-private$jobs[[jobnr]]
      return(job)
    },

    get_first_job=function() {
      job<-private$jobs_[[1]]
      return(job)
    },

    get_jobnr_by_name=function(job_name) {
      pos <- which(names(private$jobs_) %in% job_name)
      if(length(pos)==0) {
        return(numeric())
      }
      return(pos)
    },

    get_job_by_name=function(job_name) {
      pos <- which(names(private$jobs_) %in% job_name)
      if(length(pos)==0) {
        return(NULL)
      }
      if(length(pos)>1) {
        return(private$jobs_[pos])
      } else {
        return(private$jobs_[[pos]])
      }
    },

    get_job_by_nr=function(jobnr) {
      if(is.numeric(jobnr) && jobnr<1) {
        stop("jobnr must be positive integer")
      }
      if(jobnr>length(private$jobs_)) {
        stop(paste0("jobnr must be smaller or equal to ", length(private$jobs_), ", the total number of jobs"))
      }
      return(private$jobs_[[jobnr]])
    },

    get_job_count=function() {
      return(length(private$jobs_))
    },

    get_finished_job_count=function() {
      return(private$last_finished_job_)
    },

    get_queued_job_count=function() {
      return(length(private$jobs_) - private$last_finished_job_ - self$is_job_running())
    }

  ),


  private=list(

    get_stats_before_enqueue=function() {
      if(self$is_job_running()){
        return(private$jobs_[[length(private$jobs_)]])
      } else {
        return(private$get_stats_function_())
      }
    },

    create_job=function(job_name, command, flag_init_job=FALSE) {
      stats<-private$get_stats_before_enqueue()
      newjob <- JobEntry$new(job_name=job_name,
                             stats_before=stats,
                             command = command,
                             flag_init_job=flag_init_job)
      private$jobs_<-c(private$jobs_, setNames(list(newjob), job_name))
      return(newjob)
    },

    jobs_=list(),
    last_finished_job_=0,
    get_stats_function_=NA
  ),

  cloneable = FALSE,
  lock_class = TRUE
)


JobEntry<-R6::R6Class("JobEntry",
  public = list(
    initialize=function(job_name, stats_before, command=NULL, flag_init_job=FALSE) {
#      browser()
      if(!flag_init_job) {
        private$job_ <- BackgroundTask$new()
      } else {
        private$stats_after_ <- stats_before
      }
      private$ans_<-simpleError("This job was never run")
      private$job_name_ <- job_name
      if(!is.null(command)) {
        private$command_ <- command
      }
      private$stats_before_ <- stats_before
    },

    is_task_finished=function() {
      if(is.environment(private$job_)) {
        if(private$job_$is_task_running()) {
          return(FALSE)
        } else {
          ans <- private$job_$get_task_return_value(flag_clear_memory=TRUE)
          if(!is.null(ans)) {
            if(!'try-error' %in% class(ans)) {
              private$stats_before2_ <- ans$start_stats
              private$stats_after_ <- ans$end_stats
              if(length(ans$ans)==1){
                if(is.environment(ans$ans)) {
                  private$ans_ <- ans$ans[[names(ans$ans)]]
                } else {
                  private$ans_ <- ans$ans[[1]]
                }
              } else {
                private$ans_ <- ans$ans
              }
              private$job_ <- NA
            } else {
              browser()
              private$ans_ <- ans
            }
          } else {
            browser()
          }
          return(TRUE)
        }
      } else{
        return(TRUE)
      }
    },

    get_return_value=function(flag_clear_memory=TRUE) {
      if(!self$is_task_finished()) {
        return(simpleError("Task is still running"))
      }
      ans<-private$ans_
      if(flag_clear_memory){
        private$ans_<-simpleError("Return value was cleared")
      }
      return(ans)
    },

    get_job_stats_after=function() {
      if(!self$is_task_finished()) {
        return(simpleError("Task is still running"))
      }
      return(private$stats_after_)
    },

    get_job_stats_before=function() {
      if(class(private$stats_before_)=='JobEntry') {
        if(private$stats_before_$is_task_finished()) {
          private$stats_before_ <- private$stats_before_$get_job_stats_after()
        } else {
          return(simpleError("Task has not started executing"))
        }
      }
      return(private$stats_before_)
    },

    wait_until_finished=function(timeout=0) {
      if(self$is_task_finished()){
        return(TRUE)
      }
      private$job_$wait_for_task_finish(timeout=timeout)
    }

  ),

  active = list(
    tag = function(newtag) {
      if(missing(newtag)) {
        return(private$tag_)
      } else {
        private$tag_ <- newtag
      }
    },
    command = function(newcommand) {
      if(missing(newcommand)) {
        return(private$command_)
      } else {
        private$command_ <- newcommand
      }
    },
    name = function() private$job_name_,
    job = function() return(private$job_)
  ),

  private = list(
    job_=NA,
    ans_=NA,
    job_name_=NA,
    command_=NA,
    tag_=NA,
    stats_before_=NA,
    stats_before2_=NA,
    stats_after_=NA

  ),

  cloneable = FALSE,
  lock_class = TRUE
)
