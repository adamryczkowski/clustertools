library(clustertools)

library(testthat)

source("remote_host.R")

context(paste0('Executing two jobs on ', remote_host))

test_that(paste0("Scheduling two tasks with the same name on ", remote_host), {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)
  a1<-srv_loc$execute_job(expression = 2+2 , job_name = "job", timeout = 0, flag_clear_memory = FALSE)
  a2<-srv_loc$execute_job(expression = 3+3 , job_name = "job", timeout = 0, flag_clear_memory = FALSE)

  a<-srv_loc$get_job_by_name('job')

  expect_equal(length(a),2)
  expect_equal(a[[1]]$peek_return_value(), 2+2)
  expect_equal(a[[2]]$peek_return_value(), 3+3)
  srv_loc$finalize()
})

