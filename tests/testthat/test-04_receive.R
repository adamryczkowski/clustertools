library(clustertools)

library(testthat)



context('Receiving from remote')

test_that("Receiving object from remote", {
  #todo
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
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
  gc()
})

