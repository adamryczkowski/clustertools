library(clustertools)

library(testthat)

source("remote_host.R")

context(paste0('Transfer files to ', remote_host))

prepare_file<-function(n=1000) {
  tmp<-list(payload=rnorm(n), name="tempobj")
  loctmpdir1<-base::tempdir()
  saveRDS(tmp, file = file.path(loctmpdir1, 'file.rds'), compress=FALSE)
  return(list(dir=loctmpdir1, file='file.rds'))
}

test_that(paste0("Simple transfer of file to ", remote_host), {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)
  fileinfo<-prepare_file()
  expect_true(is.numeric(srv_loc$send_file(local_path =file.path(fileinfo$dir, fileinfo$file), remote_path = file.path(fileinfo$dir, "file2.rds"), flag_wait = TRUE)))

  srv_loc$finalize()
})

test_that(paste0("Transfer of file that requires new directory to ", remote_host), {
  gc()
  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)
  fileinfo<-prepare_file()
  expect_true(is.numeric(srv_loc$send_file(local_path =file.path(fileinfo$dir, fileinfo$file),
                                          remote_path = file.path(paste0(fileinfo$dir, "_remote"), fileinfo$file), flag_wait = TRUE)))

  srv_loc$finalize()
})
