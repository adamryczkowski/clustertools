library(clustertools)

library(testthat)

context('CPU load')

test_that("Test CPU loading tasks", {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
#  debugonce(srv_loc$execute_job)

  busy_sleep<-function(time) {
    start<-as.numeric(Sys.time())
    a<-0
    while(as.numeric(Sys.time()) - start < time) {a<-a+1}
  }

  a1<-srv_loc$send_objects(list(busy_sleep=busy_sleep))

  a1<-srv_loc$execute_job(job_name = "CPULOAD1",expression = busy_sleep(2.4))
  Sys.sleep(2)
  expect_gt(a1$get_current_statistics()$cpuload, expected = 0.9)

  a2<-srv_loc$execute_job(job_name = "CPULOAD2",expression = busy_sleep(0.3))
  a3<-srv_loc$execute_job(job_name = "CPULOAD3",expression = busy_sleep(0.3))


  expect_equal(a1$get_current_statistics()$state, 'running')
  expect_equal(a2$get_current_statistics()$state, 'scheduled')
  expect_equal(a3$get_current_statistics()$state, 'scheduled')

  a1$pop_return_value(flag_wait_until_finished = TRUE)

  expect_equal(a1$get_current_statistics()$state, 'finished')
  expect_gt(a1$get_current_statistics()$cpuload, expected = 0.7)
  expect_equal(a2$get_current_statistics()$state, 'running')
  expect_equal(a3$get_current_statistics()$state, 'scheduled')

  a3$pop_return_value(flag_wait_until_finished = TRUE)

  expect_gt(srv_loc$get_total_load()$cpu_time, 2.9)

  srv_loc$finalize()
})

test_that("Multi-CPU task", {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
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

    expect_gt(system.time(busy_sleep_ncpu(1, 2))[4], 1.1)

    #  debugonce(srv_loc$execute_job)

    a1<-srv_loc$send_objects(list(busy_sleep_ncpu=busy_sleep_ncpu))

    a1<-srv_loc$execute_job(job_name = "CPULOAD1",expression = busy_sleep_ncpu(2.5, 2))
    Sys.sleep(2)
    a1
    expect_gt(a1$get_current_statistics()$cpuload, expected = 1.8)

    a1$pop_return_value(flag_wait_until_finished = TRUE)

    expect_gt(srv_loc$get_total_load()$cpu_time, 4.6)
  }

  srv_loc$finalize()

})
