library(clustertools)

library(testthat)

#setwd('tests/testthat')
source("remote_host.R")

#futile.logger::flog.appender(futile.logger::appender.file("/tmp/log.txt"))
#futile.logger::flog.threshold(futile.logger::INFO)
#unlink('/tmp/log.txt')

context(paste0('Executing big transfer on ', remote_host))

test_that(paste0("Executing a big transfer on ", remote_host), {
  gc()

  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)

  rec_cnt<-10^6
#  rec_cnt<-10
  large_object<-data.frame(norm=round(rnorm(rec_cnt, 100,15), digits=5), fact=sample(factor(1:5, labels = c('Very low', 'Low', 'Middle', 'High', 'Very high')), size = rec_cnt, replace=TRUE))

  #debug(environment(srv_loc$print)$private$execute_wait_)
  #debug(environment(srv_loc$print)$private$execute_)
  #debug(srv_loc$send_objects)
  a1<-srv_loc$send_objects(named_list_of_objects = list(obj=large_object), job_name = 'sending', timeout = 0)

  a2<-srv_loc$execute_job(job_name = 'check', expression = object.size(obj), timeout = 0)
  expect_equal(a2, as.numeric(object.size(large_object)))

#  debugonce(srv_loc$receive_objects)
  a3<-srv_loc$receive_objects(object_names = c("obj") , job_name = "receiving", timeout = 0)

  expect_equivalent(large_object, a3)
  srv_loc$finalize()
})

