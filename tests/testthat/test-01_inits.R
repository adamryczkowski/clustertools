library(clustertools)

library(testthat)

source("remote_host.R")

context(paste0("Local cluster initialization with ", remote_host))

test_that(paste0("Server start and stop on ", remote_host), {
  gc()
  srv_loc<-RemoteServer$new(remote_host)
  expect_equal(srv_loc$host_address, remote_host)
  #expect_equal(srv_loc$host_name, system('hostname', intern = TRUE))
  expect_equal(srv_loc$get_count_statistics()$total, expected = 0)
  expect_equal(srv_loc$get_count_statistics()$finished, expected = 0)
  expect_equal(srv_loc$get_count_statistics()$queued, expected = 0)
  expect_null(srv_loc$get_last_job())
  expect_length(srv_loc$get_current_load(), n=3)
  expect_error(srv_loc$get_job_return_value('bla'))
  expect_false(srv_loc$is_busy())
  srv_loc$finalize()
})

