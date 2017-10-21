library(clustertools)

library(testthat)

source("remote_host.R")

context(paste0('CPU load on ', remote_host))

test_that(paste0("Test CPU loading tasks on ", remote_host), {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)
#  debugonce(srv_loc$execute_job)

  busy_sleep<-function(time) {
    start<-as.numeric(Sys.time())
    a<-0
    while(as.numeric(Sys.time()) - start < time) {a<-a+1}
  }

  a1<-srv_loc$send_objects(list(busy_sleep=busy_sleep))

  a1<-srv_loc$execute_job(job_name = "CPULOAD1",expression = busy_sleep(2.4))
  Sys.sleep(2)
  expect_gt(a1$get_current_statistics()$cpuload, expected = 0.5)

  a2<-srv_loc$execute_job(job_name = "CPULOAD2",expression = busy_sleep(0.3))
  a3<-srv_loc$execute_job(job_name = "CPULOAD3",expression = busy_sleep(0.3))


  expect_equal(a1$get_current_statistics()$state, 'running')
  expect_equal(a2$get_current_statistics()$state, 'scheduled')
  expect_equal(a3$get_current_statistics()$state, 'scheduled')

  a1$pop_return_value(flag_wait_until_finished = TRUE)

  expect_equal(a1$get_current_statistics()$state, 'finished')
  expect_gt(a1$get_current_statistics()$cpuload, expected = 0.5)
  expect_equal(a2$get_current_statistics()$state, 'running')
  expect_equal(a3$get_current_statistics()$state, 'scheduled')

  a3$pop_return_value(flag_wait_until_finished = TRUE)

  expect_gt(srv_loc$get_total_load()$cpu_time, 2.5)

  srv_loc$finalize()
})

test_that(paste0("Multi-CPU task on ", remote_host), {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)
  srv_loc$wait_for_all_tasks()
  if(srv_loc$cpu_cores>1) {
    busy_sleep_ncpu<-function(time, ncpu) {
      busy_sleep<-function(time) {
        start<-as.numeric(Sys.time())
        a<-0
        while(as.numeric(Sys.time()) - start < time) {a<-a+1}
      }
      j<-list()
      for(i in seq(ncpu)) {
        j[[i]]<-parallel::mcparallel(busy_sleep(time))
      }
      parallel::mccollect(j);1
    }

#    time_check<-system.time(busy_sleep_ncpu(1, 2))[4]
#    expect_gt(time_check, 1.1)

    #  debugonce(srv_loc$execute_job)

    a1<-srv_loc$send_objects(list(busy_sleep_ncpu=busy_sleep_ncpu))

    a1<-srv_loc$execute_job(job_name = "CPULOAD1",expression = busy_sleep_ncpu(2.5, 2))
    Sys.sleep(2)
    a1
    expect_gt(a1$get_current_statistics()$cpuload, expected = 0.5) #Under ideal circumstances the timing should be just a bit less than 2.0.

    a1$pop_return_value(flag_wait_until_finished = TRUE)

    expect_gt(srv_loc$get_total_load()$cpu_time, 3.9)
  }

  srv_loc$finalize()

})
