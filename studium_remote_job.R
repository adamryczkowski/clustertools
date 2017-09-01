library(yuxia)
srv1<-RemoteServer$new("rstudio")
srv2<-RemoteServer$new('10.29.153.100')

srv1$get_pid()

srv1$send_file(local_path = 'nominal_ts_sample_mydt.rds', remote_path = '/tmp/nominal_ts_sample_mydt.rds', flag_wait = TRUE)


debugonce(send_big_objects)
dt<-readRDS('nominal_ts_sample_mydt.rds')

srv1$send_objects(named_list_of_objects = list(dt2=dt), flag_wait = TRUE)

cl<-srv1$cl_connection
parallel::clusterEvalQ(cl, object.size(dt2))
send_big_objects(cl,objects = list(dt2=dt))
srv1$job$is_task_running()

a=get_cpu_capabilies(cl)
perfscript_remote_path<-parallel::clusterEvalQ(cl, {
  tmpfile_txt <- tempfile(fileext = '.sh')
  tmpfile <- file(tmpfile_txt)
  script='#!/usr/bin/env bash
pgid=$(ps -o pgid= $1)
sizes() { /bin/ps -o rss= -$1;}
peak=0
while sizes=$(sizes $pgid)
do
    set -- $sizes
    sample=$((${@/#/+}))
    let peak="sample > peak ? sample : peak"
    sleep 0.1
done
echo "$peak" >&2
'
  writeLines(script, tmpfile)
  close(tmpfile)
  Sys.chmod(tmpfile_txt, mode = "0777", use_umask = TRUE)
  return(tmpfile_txt)
})

c(a, perfscript_remote_path=perfscript_remote_path)

srv1$job$get_task_return_value()

srv1$send_file(local_path = 'nominal_ts_sample_mydt.rds', remote_path = '/tmp/nominal_ts_sample_mydt.rds')

cl<-srv1$cl_connection
send_file(cl, file_path='nominal_ts_sample_mydt.rds', remote_path='/tmp/nominal_ts_sample_mydt.rds', flag_check_first=TRUE)

srv1$job$run_task(
  {
    a=get_cpu_capabilies(cl)
    perfscript_remote_path<-parallel::clusterEvalQ(cl, {
      tmpfile_txt <- tempfile(fileext = '.sh')
      tmpfile <- file(tmpfile_txt)
      script='#!/usr/bin/env bash
pgid=$(ps -o pgid= $1)
sizes() { /bin/ps -o rss= -$1;}
peak=0
while sizes=$(sizes $pgid)
do
    set -- $sizes
    sample=$((${@/#/+}))
    let peak="sample > peak ? sample : peak"
    sleep 0.1
done
echo "$peak" >&2
'
      writeLines(script, tmpfile)
      close(tmpfile)
      Sys.chmod(tmpfile_txt, mode = "0777", use_umask = TRUE)
      return(tmpfile_txt)
    })
    c(a, perfscript_remote_path=perfscript_remote_path)
  })

srv1$job$get_task_return_value()

remote_fn<-function(df) {
  l<-list(nrow=nrow(df),
          sig=system('hostname', intern = TRUE))
  return(l)
}

