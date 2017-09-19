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
})

test_that("Copying object to remote", {
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
  a<-srv_loc$send_objects(named_list_of_objects = list(a=1:10))
  expect_equal(a$peek_return_value(flag_wait_until_finished = TRUE), "1 object sent.")
  a2<-srv_loc$execute_job('get_a', a, flag_wait = TRUE, flag_clear_memory = FALSE)
  expect_equivalent(a2, 1:10)

  a<-srv_loc$send_objects(named_list_of_objects = list(a=3:5, b="string"))
  expect_equal(a$peek_return_value(flag_wait_until_finished = TRUE), "2 objects sent.")

  a2<-srv_loc$execute_job('get_a', a, flag_wait = TRUE, flag_clear_memory = FALSE)
  expect_equivalent(a2, 3:5)
  a2<-srv_loc$execute_job('get_b', b, flag_wait = TRUE, flag_clear_memory = FALSE)
  expect_equivalent(a2, "string")

})

test_that("Receiving object from remote", {
  #todo
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
  a<-srv_loc$send_objects(job_name = "send", named_list_of_objects = list(a=10:14, b="second string"), flag_wait = TRUE)
  debugonce(srv_loc$receive_objects)
  a<-srv_loc$receive_objects(object_names = "a" , job_name = "receive")
  ans<-a$peek_return_value()
  expect_equal(class(ans), 'environment')
  expect_equal(length(ans), 1)
  expect_equivalent(ans$a, 10:14)

  a<-srv_loc$receive_objects(object_names = c("a","b") , job_name = "receive")
  ans<-a$peek_return_value()
  expect_equal(class(ans), 'environment')
  expect_equal(length(ans), 2)
  expect_equivalent(ans$a, 10:14)
  expect_equivalent(ans$b, "second string")
})
#Przy uruchomieniu serwera zostaje utworzony prywatny katalog /tmp/<random> na serwerze.
#W nim są umieszczane skrypty z katalogu 'scripts' w ścieżce pakietu.
#Poza tym zostaje uruchomiony w tle (detached) proces `peak_mem.sh`.
#
#Po wysłaniu nietrywialnej komendy do uruchomienia na serwerze, najpierw zostaje uruchomiony skrypt
#Proces liczy
#Ten proces

