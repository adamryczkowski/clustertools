
BackgroundTask<-R6::R6Class("BackgroundTask",
 public = list(
   initialize=function() {

   },
   run_task=function(expr) {
     if(self$is_task_running()) {
       stop("Another background task is already running!")
     }
     private$job_ <- parallel::mcparallel(expr)
   },
   is_task_running=function(expr) {
      taskid<-self$task_id
      if(!is.na(taskid)){
        ans<-parallel::mccollect(private$job_, wait=FALSE)
        if(is.null(ans)){
          return(TRUE)
        } else {
          private$ans_<-ans[[as.character(taskid)]]
          parallel::mccollect(private$job_, wait=TRUE)
          private$job_<-NA
          return(FALSE)
        }
      } else {
        return(FALSE)
      }
   },
   get_task_return_value=function() {
     self$wait_for_task_finish()
     return(self$task_return_value)
   },
   wait_for_task_finish=function() {
     if(self$is_task_running()){
       ans<-parallel::mccollect(private$job_, wait=TRUE)
       private$ans_<-ans[[as.character(self$task_id)]]
       private$job_<-NA
     }
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
   }
 ),
 private = list(
   job_=NA,
   ans_=NA
 )

)

# b1<-BackgroundTask$new()
# b1$is_task_running()
# #
# b1$run_task({Sys.sleep(10);2})
# b1$is_task_running()
# a<-b1$job
# b1$task_return_value
# b1$job
# b1$task_return_value
# b1$get_task_return_value()
