library(clustertools)

library(testthat)

#setwd('tests/testthat')
source("remote_host.R")

#futile.logger::flog.appender(futile.logger::appender.file("/tmp/log.txt"))
#futile.logger::flog.threshold(futile.logger::INFO)


context(paste0("Executing a simple job remotely", remote_host))

test_that(paste0("Executing a simple job remotely on ", remote_host), {
  gc()
  options(warn=2)
  set.seed(Sys.time())
  #unlink('/tmp/log.txt')
  srv_loc<-RemoteServer$new(remote_host, port=11002)
#debug(environment(srv_loc$print)$private$execute_wait_)
#debug(environment(srv_loc$print)$private$execute_)
#debug(srv_loc$execute_job)
  remote_pid<-srv_loc$execute_job(42, job_name = 'get_syspid', timeout=0, flag_clear_memory = FALSE)
  remote_pid<-srv_loc$execute_job(Sys.getpid(), job_name = 'get_syspid', timeout=0, flag_clear_memory = FALSE)

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
  expect_equal(length(a),2)
  astat2<-a[[2]]$get_current_statistics()
  expect_equivalent(astat, astat2)

  expect_equal(remote_pid, a[[2]]$peek_return_value())
  expect_equal(remote_pid, a[[2]]$pop_return_value())
  expect_error(a[[2]]$peek_return_value())
  srv_loc$finalize()
})

context('Job that causes error')

test_that(paste0("Executing a job that makes an error on ",remote_host), {
  gc()
  b<-BackgroundTask$new()
  b$run_task(expr = list()-1)
  a<-b$get_task_return_value()
  expect_true('try-error' %in% class(a))

  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)
  out<-srv_loc$execute_job(list()-1, job_name = 'error_job', timeout = -1, flag_clear_memory = FALSE)

  expect_error(out$peek_return_value(flag_wait_until_finished = TRUE))

  astat<-out$get_current_statistics()
  expect_true("list" %in% class(astat))
  srv_loc$wait_for_all_tasks()
  expect_equal(srv_loc$get_count_statistics()$finished, 1)
  srv_loc$finalize()
})

context('Setting and removing servers quickly')
test_that(paste0("Setting and removing servers quickly on ", remote_host), {
  gc()
  srv_loc<-RemoteServer$new(remote_host)
  remote_pid1<-srv_loc$execute_job(Sys.getpid(), job_name='get_syspid', timeout = 0, flag_clear_memory = FALSE)
  stats<- srv_loc$get_current_load()
  srv_loc$finalize()

  srv_loc<-RemoteServer$new(remote_host)
  remote_pid2<-srv_loc$execute_job(Sys.getpid(), job_name='get_syspid', timeout = 0, flag_clear_memory = FALSE)
  stats<- srv_loc$get_current_load()
  expect_false(remote_pid1 == remote_pid2)
  srv_loc$finalize()

  srv_loc<-RemoteServer$new(remote_host)
  remote_pid3<-srv_loc$execute_job(Sys.getpid(), job_name='get_syspid', timeout = 0, flag_clear_memory = FALSE)
  stats<- srv_loc$get_current_load()
  expect_false(remote_pid1 == remote_pid3)
  srv_loc$finalize()
})

