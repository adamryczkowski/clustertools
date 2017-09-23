
BackgroundTask<-R6::R6Class("BackgroundTask",
 public = list(
   initialize=function() {
   },
   run_task=function(expr, flag_log_command=FALSE) {
     if(self$is_task_running()) {
       stop("Another background task is already running!")
     }
     Sys.sleep(0.01)
#     browser()
     private$job_ <- parallel::mcparallel(expr)
     if(flag_log_command){
       private$command_ <- deparse(expr)
     } else {
       private$command_ <- NULL
     }
   },
   is_task_running=function() {
      job<-private$job_
      if('parallelJob' %in% class(job)){
        ans<-parallel::mccollect(private$job_, wait=FALSE)
        if(is.null(ans)){
          return(TRUE)
        } else {
#          browser()
          private$ans_<-ans[[as.character(self$task_id)]]
          parallel::mccollect(private$job_, wait=TRUE)
          private$job_<-NA
          return(FALSE)
        }
      } else {
        return(FALSE)
      }
   },
   get_task_return_value=function(flag_clear_memory=FALSE) {
     self$wait_for_task_finish()
     ans<-self$task_return_value
     if(flag_clear_memory) {
       private$ans_ <- NULL
     }
     return(ans)
   },

   #Returns TRUE if task has finished
   #FALSE if task is still executing
   wait_for_task_finish=function(timeout=0) {
     if(self$is_task_running()){
       if(timeout>0) {
         ans<-parallel::mccollect(private$job_, wait=FALSE, timeout=timeout)
       } else {
         ans<-parallel::mccollect(private$job_, wait=TRUE)
       }
#       browser()
       if(is.null(ans)) {
         return(FALSE)
       }
       private$ans_<-ans[[as.character(self$task_id)]]
       private$job_<-NA
     }
     return(TRUE)
   }
 ),
 active = list(
   job = function() {private$job_},
   task_id=function() {
     if(!'parallelJob' %in% class(private$job_)){
       return(NA)
     }
     return(private$job_$pid)
   },
   task_return_value=function() {
     if(self$is_task_running()){
        return(NULL)
     } else {
        return(private$ans_)
     }
   },
   last_command=function() {
     return(private$command_)
   }
 ),
 private = list(
   job_=NA,
   command_=NA,
   ans_='Not initialized'
 )

)
