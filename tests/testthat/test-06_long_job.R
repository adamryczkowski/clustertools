library(clustertools)

library(testthat)

source("remote_host.R")

context(paste0('Executing long job on ', remote_host))

test_that(paste0("Scheduling long job on ", remote_host), {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)
  a<-system.time(srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(1), timeout = 0))
  expect_lt(a[[3]],1.7)
  expect_gt(a[[3]],1)

  t<-system.time(srv_loc$execute_job(job_name = 'long2', expression = Sys.sleep(3), timeout = 1))
  expect_lt(t[[3]],1.6)
  expect_gt(t[[3]],1)
  a<-srv_loc$get_job_by_name('long2')
  expect_true('RemoteJob'%in%class(a))
  expect_true(a$is_running())
  t<-system.time(a$peek_return_value(flag_wait_until_finished = TRUE, timeout = 1))
  expect_lt(t[[3]], 1.6)
  expect_gt(t[[3]], 1)
  expect_true(a$is_running())
  t<-system.time(a$peek_return_value(flag_wait_until_finished = TRUE))
  expect_lt(t[[3]], 1.6)
  expect_gt(t[[3]], 1)
  expect_false(a$is_running())
  srv_loc$finalize()
})

