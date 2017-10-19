library(clustertools)

library(testthat)

source("remote_host.R")

context(paste0('Long queue on ', remote_host))

test_that(paste0("Checking the timing of the tasks on ", remote_host), {
  gc()


  srv_loc<-RemoteServer$new(remote_host)
  bt<-system.time(
    for(i in 1:10) {
      srv_loc$execute_job(2+2, job_name = 'benchmark', timeout = 0)
    })[[3]]/10

  expect_lt(bt, 1)
  expect_gt(bt, 0)

  a<-list()
  for(i in 1:5) {
    a[[i]]<-eval(substitute(srv_loc$execute_job(job_name = 'short', expression = {
      Sys.sleep(1+i/100)
    }, timeout = -1),
    list(i=i)))
  }


  t<-system.time(a[[1]]$peek_return_value(flag_wait_until_finished = TRUE))
  expect_lt(t[[3]],1.5+bt)
  expect_gt(t[[3]],0.5+bt)

  t<-system.time(a[[3]]$peek_return_value(flag_wait_until_finished = TRUE))
  expect_lt(t[[3]],2.5+bt)
  expect_gt(t[[3]],1.5+bt)

  t<-system.time(a[[5]]$peek_return_value(flag_wait_until_finished = TRUE))
  expect_lt(t[[3]],2.5+bt)
  expect_gt(t[[3]],1.5+bt)

  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)
  a1<-srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(1), timeout = -1)
  expect_true(a1$is_running())
#  debugonce(srv_loc$execute_job)
  t<-system.time(srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(1), timeout = 0))
  expect_lt(t[[3]],2.5+bt)
  expect_gt(t[[3]],1.5+bt)
  expect_true(a1$is_finished())



  srv_loc<-RemoteServer$new(remote_host)
  #debugonce(srv_loc$execute_job)
  a1<-srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(0.9))
  # m<-srv_loc$mutex
  # synchronicity::unlock(m)
  expect_true(a1$is_running())
  a2<-srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(1.1))
  expect_true(a2$is_scheduled())
  #debugonce(a2$peek_return_value)
  t<-system.time(a2$peek_return_value(flag_wait_until_finished = TRUE))
  expect_lt(t[[3]],2.5+bt)
  expect_gt(t[[3]],1.5+bt)
  expect_true(a1$is_finished())
  expect_true(a2$is_finished())


  srv_loc<-RemoteServer$new(remote_host)
  a1<-srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(1))
  expect_true(a1$is_running())
  a2<-srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(10))
  expect_true(a2$is_scheduled())
  t<-system.time(a1$peek_return_value(flag_wait_until_finished = TRUE))
  expect_lt(t[[3]],1.5+bt)
  expect_gt(t[[3]],0.5+bt)
  expect_true(a1$is_finished())
  expect_true(a2$is_running())
  srv_loc$finalize()
})

