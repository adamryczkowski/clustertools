library(clustertools)

library(testthat)



context('Executing two jobs')

test_that("Scheduling two tasks with the same name", {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
  a1<-srv_loc$execute_job(expression = 2+2 , job_name = "job", flag_wait = TRUE, flag_clear_memory = FALSE)
  a2<-srv_loc$execute_job(expression = 3+3 , job_name = "job", flag_wait = TRUE, flag_clear_memory = FALSE)

  a<-srv_loc$get_job_by_name('job')

  expect_equal(length(a),2)
  expect_equal(a[[1]]$peek_return_value(), 2+2)
  expect_equal(a[[2]]$peek_return_value(), 3+3)
  gc()
})

