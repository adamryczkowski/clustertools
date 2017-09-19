library(clustertools)

library(testthat)

context("Local cluster initialization")

test_that("Server start and stop", {
  srv_loc<-RemoteServer$new('localhost')
  expect_equal(srv_loc$host_address, 'localhost')
  expect_equal(srv_loc$host_name, system('hostname', intern = TRUE))
  expect_equal(srv_loc$get_count_statistics()$total, expected = 0)
  expect_equal(srv_loc$get_count_statistics()$finished, expected = 0)
  expect_equal(srv_loc$get_count_statistics()$queued, expected = 0)
  expect_null(srv_loc$get_last_job())
  expect_length(srv_loc$get_current_load(), n=3)
  expect_error(srv_loc$get_job_return_value('bla'))
  expect_false(srv_loc$is_busy())
})

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
})

test_that("Copying object to remote", {
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
  browser()
  srv_loc$send_objects(named_list_of_objects = list(a=1:10))
  srv_loc$spawn_job(named_list_of_objects = list(a=1:10))
  debugonce(RemoteServer$new)

})
#Przy uruchomieniu serwera zostaje utworzony prywatny katalog /tmp/<random> na serwerze.
#W nim są umieszczane skrypty z katalogu 'scripts' w ścieżce pakietu.
#Poza tym zostaje uruchomiony w tle (detached) proces `peak_mem.sh`.
#
#Po wysłaniu nietrywialnej komendy do uruchomienia na serwerze, najpierw zostaje uruchomiony skrypt
#Proces liczy
#Ten proces

