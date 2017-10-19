library(clustertools)

library(testthat)

#source("remote_host.R")

context("Testing background job")

test_that("BackgroundTask tests", {
  gc()
  bj<-BackgroundTask$new()
  bj$run_task(2+2)
  bj$wait_for_task_finish()
  expect_equal(bj$get_task_return_value(), 4)
  e<-new.env()
  e$a<-1
  a<-2
  bj$run_task(a+1, e)
  bj$wait_for_task_finish()
  expect_equal(bj$get_task_return_value(), 2)

  bj$run_task(Sys.sleep(0.1))
  expect_true(bj$is_task_running())
  bj$wait_for_task_finish()
})

