library(clustertools)

library(testthat)

source("remote_host.R")

context(paste0('Sending to ', remote_host) )

test_that(paste0("Copying object to ", remote_host), {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)
  a<-srv_loc$send_objects(named_list_of_objects = list(a=1:10))
  expect_equal(a$peek_return_value(flag_wait_until_finished = TRUE), "1 object sent.")
  a2<-srv_loc$execute_job(a, job_name = 'get_a', flag_wait = TRUE, flag_clear_memory = FALSE)
  expect_equivalent(a2, 1:10)

  a<-srv_loc$send_objects(named_list_of_objects = list(a=3:5, b="string"))
  expect_equal(a$peek_return_value(flag_wait_until_finished = TRUE), "2 objects sent.")

  a2<-srv_loc$execute_job(a, job_name = 'get_a', flag_wait = TRUE, flag_clear_memory = FALSE)
  expect_equivalent(a2, 3:5)
  a2<-srv_loc$execute_job(b, job_name = 'get_b', flag_wait = TRUE, flag_clear_memory = FALSE)
  expect_equivalent(a2, "string")

  srv_loc$finalize()
})

