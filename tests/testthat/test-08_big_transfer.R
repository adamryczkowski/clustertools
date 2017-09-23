library(clustertools)

library(testthat)

context('Executing big transfer')

test_that("Executing a big transfer", {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')

  rec_cnt<-10^6
#  rec_cnt<-10
  large_object<-data.frame(norm=round(rnorm(rec_cnt, 100,15), digits=5), fact=sample(factor(1:5, labels = c('Very low', 'Low', 'Middle', 'High', 'Very high')), size = rec_cnt, replace=TRUE))

  a1<-srv_loc$send_objects(named_list_of_objects = list(obj=large_object), job_name = 'sending', flag_wait = TRUE)

  a2<-srv_loc$execute_job(job_name = 'check', expression = object.size(obj), flag_wait = TRUE)
  expect_equal(a2, object.size(large_object))

#  debugonce(srv_loc$receive_objects)
  a3<-srv_loc$receive_objects(object_names = c("obj") , job_name = "receiving", flag_wait = TRUE)

  expect_equivalent(large_object, a3)
  srv_loc$finalize()
})

