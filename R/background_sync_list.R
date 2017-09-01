# This class executes list of scheduled jobs sequentially, and collects their results
#
# Each scheduled task has a forked job, which ID is added to the private$jobs_ list.
# The job itself first waits for the start mutex, then executes the cluster command,
# then it frees its mutex, triggering start of the subsequent job, and at the end returns
# the remote's result


BackgroundSyncList<-R6::R6Class("BackgroundSyncList",
  public = list(
    initialize=function() {

    },
    schedule_task=function() {}
  ),
  active = list(
  ),
  private = list(
    last_mutex=NA, #Mutex that will be freed when the last job finish
    jobs=NA #list of the BackgroundTask job pointers. Keys are names.
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
