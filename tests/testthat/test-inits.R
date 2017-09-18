library(clustertools)

library(testthat)

context("Local cluster initialization")

test_that("Server start and stop", {
  srv_loc<-RemoteServer$new('localhost')
  expect_equal(srv_loc$host_address, 'localhost')
  expect_equal(srv_loc$host_name, system('hostname', intern = TRUE))
})

test_that("Sending and receiving file from remote", {
  options(warn=2)
  srv_loc<-RemoteServer$new('localhost')
  debugonce(srv_loc$execute_job)
  srv_loc$execute_job('', Sys.getpid(), flag_wait = TRUE)
  debugonce(srv_loc$get_last_job)
  a<-srv_loc$get_last_job()

  a

  debugonce(srv_loc$execute_job)
  srv_loc$execute_job('', "KUKU!", flag_wait = TRUE)
  expect_lt(srv_loc$get_current_load()$memkb, 60000)

  debugonce(srv_loc$print)
  srv_loc$print()
  paste_scripts_to_server(srv_loc$cl_aux_connection)


  srv_loc$send_objects(named_list_of_objects = list(a=1:10))
  srv_loc$spawn_job(named_list_of_objects = list(a=1:10))
  debugonce(RemoteServer$new)
})



#Przy uruchomieniu serwera zostaje utworzony prywatny katalog /tmp/<random> na serwerze.
#W nim są umieszczane skrypty z katalogu 'scripts' w ścieżce pakietu.
#Poza tym zostaje uruchomiony w tle (detached) proces `peak_mem.sh`.
#
#Po wysłaniu nietrywialnej komendy do uruchomienia na serwerze, najpierw zostaje uruchomiony skrypt
#Proces liczy
#Ten proces
