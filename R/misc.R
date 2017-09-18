find_default_if<-function(){
  return(system("/sbin/ip route | awk '/default/ { print $5 }'", intern=TRUE))
}

get_cpu_capabilies<-function(cl) {
  capabilities<-parallel::clusterEvalQ(cl, list(memkb=system("grep MemTotal /proc/meminfo | awk '{print $2}'", intern = TRUE),
                                                cores=system('grep "^core id" /proc/cpuinfo | sort -u | wc -l', intern=TRUE),
                                                speed=1/system.time(system('dd if=/dev/zero bs=3 count=1000000 2>/dev/null  | md5sum  >/dev/null'))[[3]],
                                                host_name=system('hostname', intern = TRUE)
  ))
  capabilities<-capabilities[[1]]
  capabilities$memkb<-as.numeric(capabilities$memkb)
  capabilities$cores<-as.numeric(capabilities$cores)

  obj<-readBin('/dev/urandom', what=raw(), n=2*1000*1000)
  e<-new.env()
  assign('obj', obj, envir = e)
  capabilities$net_send_speed<-1/(system.time(parallel::clusterExport(cl, 'obj', envir = e))[[3]]/2000)
  capabilities$net_receive_speed<-1/(system.time(parallel::clusterEvalQ(cl, obj))[[3]]/2000)

  return(capabilities)
}


get_current_load<-function(cl, script_dir, pid, flag_include_top=FALSE) {
  if(flag_include_top) {
    stats<-eval(substitute(
      parallel::clusterEvalQ(cl, list(
        cpuload=as.numeric(system("LC_NUMERIC=\"en_GB.UTF-8\"; top -b -d 0.3 -n2 | grep \"Cpu(s)\" 2>/dev/null | awk '{print $2+$4}' | tail -n1", intern = TRUE)),
        freememkb=as.numeric(system("grep MemAvailable /proc/meminfo | awk '{print $2}'", intern = TRUE)),
        memkb=as.numeric(system2(file.path(script_dir, 'get_current_mem.sh'),stdout=TRUE)),
        peakmemkb=as.numeric(system2(file.path(script_dir, 'get_peak_mem.sh'),stdout=TRUE)),
        cpu_time=as.numeric(system2(file.path(script_dir, 'current_time.sh'), args = pid ,stdout=TRUE)),
        wall_time=as.numeric(Sys.time())
      ))[[1]],
      list(script_dir=script_dir, pid=pid)))
  } else {
    stats<-eval(substitute(
      parallel::clusterEvalQ(cl, list(
        freememkb=as.numeric(system("grep MemAvailable /proc/meminfo | awk '{print $2}'", intern = TRUE)),
        memkb=as.numeric(system2(file.path(script_dir, 'get_current_mem.sh'),stdout=TRUE)),
        peakmemkb=as.numeric(system2(file.path(script_dir, 'get_peak_mem.sh'),stdout=TRUE)),
        cpu_time=as.numeric(system2(file.path(script_dir, 'current_time.sh'), args = pid ,stdout=TRUE)),
        wall_time=as.numeric(Sys.time())
      ))[[1]],
      list(script_dir=script_dir, pid=pid)))
  }
  return(stats)
}

benchmark_speed_compression<-function(cl, obj_size=1000) {
  #First we prepare a large typical object
  eval(substitute(parallel::clusterEvalQ(cl,{df<-data.frame(a=runif(obj_size));1}), list(obj_size=obj_size)))
  speeds<-list()
  speeds$xz<-system.time(a<-receive_big_object(cl, 'df', 'xz'))[[3]]
  speeds$bzip2<-system.time(a<-receive_big_object(cl, 'df', 'bzip2'))[[3]]
  speeds$gzip<-system.time(a<-receive_big_object(cl, 'df', 'gzip'))[[3]]
  speeds$none<-system.time(a<-receive_big_object(cl, 'df', 'none'))[[3]]
  speeds$raw<-system.time(a<-receive_big_object(cl, 'df', ''))[[3]]
  speeds$objsize<-parallel::clusterEvalQ(cl, object.size(df))
  cat('.')
  return(speeds)
}

create_benchmark_plot<-function(){
  obj_sizes<-ceiling(gen_geom_series(n = 20, start = 10, end = 10000000))
  obj_sizes<-unlist(purrr::map(obj_sizes, ~rep(., 3)))
  a<-purrr::map(obj_sizes, ~benchmark_speed_compression(cl, .))
  df<-tibble(xz=a %>% map_dbl('xz'), gzip=a %>% map_dbl('gzip'), none=a%>% map_dbl('none'),
             raw=a%>%map_dbl('raw'), bzip2=a%>%map_dbl('bzip2'), size=unlist(a%>%map('objsize')),
             nrows=obj_sizes)
  df2<-df %>% tidyr::gather(key='method', value='time', -nrows, -size) %>%
    group_by(size, nrows, method) %>%
    summarise(time_m=mean(time), time_min=min(time), time_max=max(time))
  ggplot(df2, mapping=aes(x=size, y=time_m, color=method)) +
    geom_point() + geom_line() +
    scale_y_log10(breaks = 10**(1:10), labels = scales::comma(10**(1:10))) +
    scale_x_log10(breaks = 10**(1:10), labels = scales::comma(10**(1:10)))
}

#Tworzy skrypty podane w zmiennej "scripts" w tymczasowym katalogu na serwerze
#Zwraca nazwę tymczasowego katalogu
copy_scripts_to_server<-function(cl) {
  mydir<-system.file('scripts', package='clustertools')
  all_script_names<-list.files(mydir, pattern='\\.sh$')
  all_scripts<-purrr::map(all_script_names, ~readLines(file.path(mydir, .)))
  names(all_scripts)<-all_script_names

  make_scripts<-function(scripts) {
    tmpdir <- base::tempdir()
    for(i in seq_along(scripts)){
      script <- scripts[[i]]
      scriptname <- names(scripts)[[i]]
      file<-base::file(file.path(tmpdir, scriptname))
      writeLines(script, file)
      base::close(file)
      Sys.chmod(file.path(tmpdir, scriptname), mode = "0777", use_umask = TRUE)
    }
    return(tmpdir)
  }
  parallel::clusterExport(cl, c('make_scripts','all_scripts'), environment())
  ans<-parallel::clusterEvalQ(cl,
                              {.tmp.dir<-make_scripts(all_scripts)
                              rm('make_scritps', 'scripts');.tmp.dir})[[1]]
  return(ans)
}

#Function spawns remote process that monitors pid
run_background_task<-function(cl, script_path, pid) {
  eval(substitute(
    parallel::clusterEvalQ(cl, system2(command = script_path, args = pid, wait=FALSE)),
    list(script_path=script_path, pid=pid)))
}

compute_load_between=function(load_before, load_after) {
  ans<-list(cpuload=(load_after$cpu_time - load_before$cpu_time)/(load_after$wall_time - load_before$wall_time),
            memkb=load_after$mem,
            peak_memkb=load_after$peak_mem,
            peak_memkb_delta=load_after$peak_mem-load_before$mem,
            memkb_delta=load_after$mem - load_before$mem,
            cputime=load_after$cpu_time - load_before$cpu_time,
            cputime=load_after$wall_time - load_before$wall_time,
            freememkb=load_after$freememkb,
            wall_time=load_after$wall_time - load_before$wall_time)
  return(ans)
}
