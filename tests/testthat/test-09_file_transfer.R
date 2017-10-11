library(clustertools)

library(testthat)

context('Transfer files')

prepare_file<-function(n=1000) {
  tmp<-list(payload=rnorm(n), name="tempobj")
  loctmpdir1<-base::tempdir()
  saveRDS(tmp, file = file.path(loctmpdir1, 'file.rds'), compress=FALSE)
  return(list(dir=loctmpdir1, file='file.rds'))
}

test_that("Simple transfer of file", {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
  fileinfo<-prepare_file()
  expect_true(is.number(srv_loc$send_file(local_path =file.path(fileinfo$dir, fileinfo$file), remote_path = file.path(fileinfo$dir, "file2.rds"), flag_wait = TRUE)))

  srv_loc$finalize()
})

test_that("Simple transfer of file", {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
  fileinfo<-prepare_file()
  expect_true(is.number(srv_loc$send_file(local_path =file.path(fileinfo$dir, fileinfo$file),
                                          remote_path = file.path(paste0(fileinfo$dir, "_remote"), fileinfo$file), flag_wait = TRUE)))

  srv_loc$finalize()
})

stop("Do tests on the basis of the following. Test the statistics.")
test_that("Background transfer of file", {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
  fileinfo<-prepare_file(10^7)
  debugonce(srv_loc$execute_job)

  a1<-srv_loc$execute_job("CPULOAD",{  i<-1000000000
  while(i>0) {i<-i-1}
  } )
  a1

  a2<-srv_loc$execute_job("CPULOAD2",{  i<-1000000000
  while(i>0) {i<-i-1}
  } )

  a3<-srv_loc$execute_job("CPULOAD3",{  i<-1000000000
  while(i>0) {i<-i-1}
  } )

  a1
  srv_loc
  debugonce(srv_loc$print)
  srv_loc$print()

  srv_loc$send_file(local_path =file.path(fileinfo$dir, fileinfo$file), remote_path = file.path(fileinfo$dir, "file3.rds"), flag_wait = FALSE)
  a
  debugonce(a$print)
  a$print()
  a<-expect_true(is.number(srv_loc$send_file(local_path =file.path(fileinfo$dir, fileinfo$file), remote_path = file.path(fileinfo$dir, "file3.rds"), flag_wait = FALSE)))

  srv_loc$finalize()
})

