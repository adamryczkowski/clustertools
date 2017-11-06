library(clustertools)

library(testthat)

futile.logger::flog.threshold(futile.logger::ERROR)
futile.logger::flog.remove('mutex.unlock')
futile.logger::flog.remove('mutex.lock')


#setwd('tests/testthat')
source("remote_host.R")
#ssh -L 8090:localhost:8090 aryczkowski@grad-dev -o ServerAliveInterval=30 -R 11011:localhost:11011
#srv_loc<-RemoteServer$new("aryczkowski@127.0.0.1:8090", port=11011, rscript="/usr/bin/Rscript")
#ssh aryczkowski@grad-dev -o ServerAliveInterval=30 -R 11012:localhost:11012
#srv_loc<-RemoteServer$new("aryczkowski@grad-dev", port='localhost:11012', rscript="/usr/bin/Rscript")
#srv_loc<-RemoteServer$new("aryczkowski@grad-dev", port=11013, rscript="/usr/bin/Rscript")
#srv_loc<-RemoteServer$new("localhost", port=11012)

#futile.logger::flog.appender(futile.logger::appender.file("/tmp/log.txt"))
#futile.logger::flog.threshold(futile.logger::INFO)

#debug(parallel::makeCluster)
#srv_loc<-parallel::makeCluster(rshcmd="ssh -p 8090", "127.0.0.1", user="aryczkowski", master="127.0.0.1", port=11011, homogeneous=TRUE, rscript="/usr/bin/Rscript")

context(paste0("Executing a simple job remotely", remote_host))

test_that(paste0("Executing a simple job remotely on ", remote_host), {
  gc()
  options(warn=2)
  set.seed(Sys.time())
  #unlink('/tmp/log.txt')
#  remote_host<-"aryczkowski@localhost:8090"
  srv_loc<-RemoteServer$new(remote_host, port=11002)
#debug(environment(srv_loc$print)$private$execute_wait_)
#debug(environment(srv_loc$print)$private$execute_)
#debug(srv_loc$execute_job)
  remote_pid<-srv_loc$execute_job(42, job_name = 'get_syspid', timeout=0, flag_clear_memory = FALSE)
  remote_pid<-srv_loc$execute_job(Sys.getpid(), job_name = 'get_syspid', timeout=0, flag_clear_memory = FALSE)

  expect_true(is.numeric(remote_pid))
  expect_true(remote_pid != Sys.getpid())
  expect_false(is.null(srv_loc$get_last_job()))

  a<-srv_loc$get_job_by_nr(srv_loc$get_count_statistics()$total)
  expect_true('RemoteJob' %in% class(a))
  expect_equal(a$peek_return_value(), remote_pid)
  expect_true(a$is_finished())
  expect_false(a$is_scheduled())
  expect_false(a$is_running())

  astat<-a$get_current_statistics()
  expect_equal(astat$state,'finished')
  expect_equal(astat$name,'get_syspid')
  expect_equal(astat$command,'Sys.getpid()')
  expect_lt(astat$cpuload, expected = 0.1)
  expect_gt(astat$mem_kb, expected = 0)
  expect_gte(astat$cpu_time, 0)
  expect_gte(astat$wall_time, 0)

  a<-srv_loc$get_job_by_name('get_syspid')
  expect_equal(length(a),2)
  astat2<-a[[2]]$get_current_statistics()
  expect_equivalent(astat, astat2)

  expect_equal(remote_pid, a[[2]]$peek_return_value())
  expect_equal(remote_pid, a[[2]]$pop_return_value())
  expect_error(a[[2]]$peek_return_value())
  srv_loc$finalize()
})

context('Job that causes error')

test_that(paste0("Executing a job that makes an error on ",remote_host), {
  gc()
  b<-BackgroundTask$new()
  b$run_task(expr = list()-1)
  a<-b$get_task_return_value()
  expect_true('try-error' %in% class(a))

  options(warn=2)
  srv_loc<-RemoteServer$new(remote_host)
  out<-srv_loc$execute_job(list()-1, job_name = 'error_job', timeout = -1, flag_clear_memory = FALSE)

  expect_error(out$peek_return_value(flag_wait_until_finished = TRUE))

  astat<-out$get_current_statistics()
  expect_true("list" %in% class(astat))
  srv_loc$wait_for_all_tasks()
  expect_equal(srv_loc$get_count_statistics()$finished, 2)
  srv_loc$finalize()
})

context("Testing for race conditions when executing lots of small jobs")
test_that(paste0("Lots of small jobs on ", remote_host), {
  gc()
  srv_loc<-RemoteServer$new(remote_host)
  srv_loc$get_job_by_nr(1)$peek_return_value(TRUE)
  srv_loc$send_objects(named_list_of_objects = list(mylist=list()))

  spawn_fn<-function(srv_loc, randnr) {

    j<-eval(substitute(srv_loc$execute_job(expression = {
      Sys.sleep(0.1)
      b<-list(value=randnr, walltime=as.numeric(Sys.time()))
      mylist<-c(mylist, list(b))
      b
      },
      expression_before={
        scheduled_time<-as.numeric(Sys.time())
        exp<-paste0(deparse(expr), collapse = '\n')
      },
      expression_after={
        ans$zero_time<-zero_time
        ans$scheduled_time<-scheduled_time
        ans["randnr"]<-randnr
        ans["expr"]<-exp
      },
      job_name = paste0("job nr ", randnr)),
                    list(randnr=randnr)))

    return(list(job=j, value=randnr))
  }

  test_fn<-function(item, verbose=FALSE) {

    ans<-item$job$peek_return_value(TRUE)
    if(verbose)
    {
      item$job$print()
    }
    if(is.list(ans)) {
      if(verbose) {
        cat("Return value: ", ans$value, " (=", ans$randnr, " on localhost)",
            ' initiated at ', ans$zero_time,
            ' scheduled to run on cluster at ', ans$scheduled_time,
            ' and evaluated at ', ans$walltime, '\n')
      }
    } else {
      if(verbose) {
        cat(paste0("WRONG Return value: ", ans, '\n'))
      } else {
        stop(paste0("WRONG Return value: ", ans, '\n'))
      }
      return()
    }

    if(length(ans$value)==0) {
      if(verbose) {
        browser()
      } else {
        stop("Wrong value")
      }
    }
    if(item$value!=ans$value) {
      if(verbose) {
        browser()
      } else {
        stop("Wrong value")
      }
    }
  }
#  debug(spawn_fn)
  flag_verbose=FALSE
  a<-spawn_fn(srv_loc, 1)
  test_fn(a, verbose = flag_verbose)

  jobs<-rep(x = list(list()),10)
  outlist<-list()


  for(i in seq(100)) {
    job_nr<-sample.int(length(jobs), 1)
    if(length(jobs[[job_nr]])!=0){
      j<-jobs[[job_nr]]
      if(flag_verbose) {
        cat(paste0("Getting job nr ", job_nr, " (", j$value, ") with queue ", j$job$count_jobs_scheduled_before(), "\n"))
      }
      test_fn(j, verbose = flag_verbose)
      if(flag_verbose)
      {
        cat(paste0('\nWall time: ', as.numeric(Sys.time()), "\n\n"))
      }
    }
    r<-runif(1)
    jobs[[job_nr]]<-spawn_fn(srv_loc,r)
    if(flag_verbose)  {
      cat(paste0("Spawning job nr ", job_nr, " (", r, ")\n"))
      jobs[[job_nr]]$job$print()
      cat(paste0('\nWall time: ', as.numeric(Sys.time()), "\n\n"))
    }
    outlist<-c(outlist, r)
  }
  outlist
  mylist<-srv_loc$execute_job(mylist)
  mylist
  srv_loc$finalize()
  gc()
})


context('Setting and removing servers quickly')
test_that(paste0("Setting and removing servers quickly on ", remote_host), {
  gc()
  srv_loc1<-RemoteServer$new(remote_host)
  remote_pid1<-srv_loc1$execute_job(Sys.getpid(), job_name='get_syspid', timeout = 0, flag_clear_memory = FALSE)
  stats<- srv_loc1$get_current_load()

  srv_loc2<-RemoteServer$new(remote_host)
  remote_pid2<-srv_loc2$execute_job(Sys.getpid(), job_name='get_syspid', timeout = 0, flag_clear_memory = FALSE)
  stats<- srv_loc2$get_current_load()
  expect_false(remote_pid1 == remote_pid2)

  srv_loc3<-RemoteServer$new(remote_host)
  remote_pid3<-srv_loc3$execute_job(Sys.getpid(), job_name='get_syspid', timeout = 0, flag_clear_memory = FALSE)
  stats<- srv_loc3$get_current_load()
  expect_false(remote_pid1 == remote_pid3)

  srv_loc1$finalize()
  srv_loc2$finalize()
  srv_loc3$finalize()
  gc()
})

