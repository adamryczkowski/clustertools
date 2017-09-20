library(clustertools)

library(testthat)



context('Executing two jobs')

test_that("Scheduling two tasks with the same name", {
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
  a1<-srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(1), flag_wait = FALSE)
  expect_true(a1$is_running())
  a2<-srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(1), flag_wait = FALSE)
  expect_true(a2$is_scheduled())
  t<-system.time(a2$peek_return_value(flag_wait_until_finished = TRUE))
  expect_lt(t[[3]],2.3)
  expect_gt(t[[3]],2)
  expect_true(a1$is_finished())
  expect_true(a1$is_finished())

  a1<-srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(1), flag_wait = FALSE)
  expect_true(a1$is_running())
  a2<-srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(1), flag_wait = FALSE)
  expect_true(a2$is_scheduled())
  t<-system.time(a1$peek_return_value(flag_wait_until_finished = TRUE))
  expect_lt(t[[3]],1.3)
  expect_gt(t[[3]],1)
  expect_true(a1$is_finished())
  expect_true(a2$is_running())



  srv_loc<-RemoteServer$new('localhost')
  a1<-srv_loc$execute_job(job_name = 'short', expression = Sys.sleep(10), flag_wait = FALSE)
  a1$get_parallelJob()
  expect_true(a1$is_running())
  a2<-srv_loc$execute_job(job_name = 'long', expression = Sys.sleep(300), flag_wait = FALSE)
  a2$get_parallelJob()
  expect_true(a2$is_scheduled())
  debugonce(a1$peek_return_value)
  t<-system.time(a1$peek_return_value(flag_wait_until_finished = TRUE))
  expect_lt(t[[3]],1.3)
  expect_gt(t[[3]],1)
  expect_true(a1$is_finished())
  expect_true(a2$is_running())


  expect_lt(a[[3]],1.3)
  expect_gt(a[[3]],1)

  t<-system.time(srv_loc$execute_job(job_name = 'long2', expression = Sys.sleep(3), flag_wait = TRUE, timeout = 1))
  expect_lt(t[[3]],1.3)
  expect_gt(t[[3]],1)
  a<-srv_loc$get_job_by_name('long2')
  expect_true('RemoteJob'%in%class(a))
  expect_true(a$is_running())
  t<-system.time(a$peek_return_value(flag_wait_until_finished = TRUE, timeout = 1))
  expect_lt(t[[3]], 1.3)
  expect_gt(t[[3]], 1)
  expect_true(a$is_running())
  t<-system.time(a$peek_return_value(flag_wait_until_finished = TRUE))
  expect_lt(t[[3]], 1.3)
  expect_gt(t[[3]], 1)
  expect_false(a$is_running())
  srv_loc$finalize()
  gc()
})

