library(clustertools)

library(testthat)


context("Executing a simple job remotely")

test_that("Executing a simple job remotely", {
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
  remote_pid<-srv_loc$execute_job('get_syspid', Sys.getpid(), flag_wait = TRUE, flag_clear_memory = FALSE)

  expect_true(is.numeric(remote_pid))
  expect_true(remote_pid != Sys.getpid())
  expect_false(is.null(srv_loc$get_last_job()))

  a<-srv_loc$get_last_job()
  expect_true('RemoteJob' %in% class(a))
  expect_equal(a$peek_return_value(), remote_pid)
  expect_true(a$is_finished())
  expect_false(a$is_scheduled())
  expect_false(a$is_running())

  astat<-a$get_current_statistics()
  expect_equal(astat$state,'finished')
  expect_equal(astat$name,'get_syspid')
  expect_equal(astat$command,'Sys.getpid()')
  expect_lt(astat$cpuload, expected = 0.1)
  expect_gt(astat$mem_kb, expected = 0)
  expect_gte(astat$cpu_time, 0)
  expect_gte(astat$wall_time, 0)

  a<-srv_loc$get_job_by_name('get_syspid')
  astat2<-a$get_current_statistics()
  expect_equivalent(astat, astat2)

  expect_equal(remote_pid, a$peek_return_value())
  expect_equal(remote_pid, a$pop_return_value())
  expect_error(a$peek_return_value())
  srv_loc$finalize()
  gc()
})

context('Job that causes error')

test_that("Executing a job that makes an error", {
  b<-BackgroundTask$new()
  b$run_task(expr = list()-1)
  a<-b$get_task_return_value()
  expect_true('try-error' %in% class(a))

  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
  out<-srv_loc$execute_job('error_job', list()-1, flag_wait = FALSE, flag_clear_memory = FALSE)

  expect_error(out$peek_return_value(flag_wait_until_finished = TRUE))

  astat<-out$get_current_statistics()
  expect_true("list" %in% class(astat))
  expect_equal(srv_loc$get_count_statistics()$finished, 1)
  srv_loc$finalize()
  gc()
})


