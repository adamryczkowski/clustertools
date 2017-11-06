library(clustertools)

library(testthat)

source("remote_host.R")

context(paste0("Local cluster initialization with ", remote_host))

test_that(paste0("Server start and stop on ", remote_host), {
  gc()
  srv_loc<-RemoteServer$new(remote_host)
  expect_equal(srv_loc$host_address, remote_host)
  #expect_equal(srv_loc$host_name, system('hostname', intern = TRUE))
  expect_equal(srv_loc$get_count_statistics()$total, expected = 1)
  expect_equal(srv_loc$get_count_statistics()$finished, expected = 0)
  srv_loc$wait_for_all_tasks()
  # init_job<-srv_loc$get_last_job()
  # init_job$peek_return_value(flag_wait_until_finished = TRUE);
  # Sys.sleep(0.1)
  #srv_loc$get_count_statistics()$finished;
  expect_equal(srv_loc$get_count_statistics()$finished, expected = 1)
  expect_equal(srv_loc$get_count_statistics()$queued, expected = 0)
  expect_length(srv_loc$get_current_load(), n=4)
  expect_error(srv_loc$get_job_return_value('bla'))
  expect_false(srv_loc$is_busy())
  srv_loc$finalize()
})

