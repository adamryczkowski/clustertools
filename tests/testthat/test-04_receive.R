library(clustertools)

library(testthat)

source("remote_host.R")

context(paste0('Receiving from ',remote_host))

test_that(paste0("Receiving object from ", remote_host), {
  gc()
  #todo
  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)
  a<-srv_loc$send_objects(job_name = "send", named_list_of_objects = list(a=10:14, b="second string"), flag_wait = TRUE)
  #debugonce(srv_loc$receive_objects)
  a<-srv_loc$receive_objects(object_names = "a" , job_name = "receive1")
  ans<-a$peek_return_value(flag_wait_until_finished = TRUE)
  expect_equal(class(ans), 'environment')
  expect_equal(length(ans), 1)
  expect_equivalent(ans$a, 10:14)

  a<-srv_loc$receive_objects(object_names = c("a","b") , job_name = "receive2")
  ans<-a$peek_return_value(flag_wait_until_finished = TRUE)
  expect_equal(class(ans), 'environment')
  expect_equal(length(ans), 2)
  expect_equivalent(ans$a, 10:14)
  expect_equivalent(ans$b, "second string")
  srv_loc$finalize()
})

