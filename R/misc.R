find_default_if<-function(target_ip=NULL){
  if(is.null(target_ip)) {
    return(system("/sbin/ip route | awk '/default/ { print $5 }'", intern=TRUE))
  } else {
    ips<-iptools::hostname_to_ip(target_ip)[[1]]
    routes<-stringr::str_split(system("/sbin/ip route", intern = TRUE), stringr::fixed(" "))
    routes_tab<-sapply(routes, function(x) c(x[[1]], x[[2]]))
    pos<-which(routes_tab[1,]=='default')
    routes_tab<-t(sapply(routes[-pos], function(x) c(x[[1]], x[[3]])))
    pos<-which(iptools::ip_in_range(rep(target_ip, nrow(routes_tab)), routes_tab[,1]))
    return(routes_tab[pos,2])
  }
}

get_cpu_capabilies<-function(cl) {
  capabilities<-MyClusterEval(cl, list(mem_kb=system("grep MemTotal /proc/meminfo | awk '{print $2}'", intern = TRUE),
                                                cores=system('grep "^core id" /proc/cpuinfo | sort -u | wc -l', intern=TRUE),
                                                speed=1/system.time(system('dd if=/dev/zero bs=3 count=1000000 2>/dev/null  | md5sum  >/dev/null'))[[3]],
                                                speed2=
                                      if(system2("which", args="sysbench", stdout=FALSE)==0) {
                                        as.numeric(system('sysbench --test=cpu --cpu-max-prime=2000 run | grep "total time:" | grep -Eo "[[:digit:].]+"', intern = TRUE))
                                      } else {
                                        ""
                                      },
                                                host_name=system('hostname', intern = TRUE)
  ))
  # capabilities<-parallel::clusterEvalQ(cl, list(mem_kb=system("grep MemTotal /proc/meminfo | awk '{print $2}'", intern = TRUE),
  #                                               cores=system('grep "^core id" /proc/cpuinfo | sort -u | wc -l', intern=TRUE),
  #                                               speed=1/system.time(system('dd if=/dev/zero bs=3 count=1000000 2>/dev/null  | md5sum  >/dev/null'))[[3]],
  #                                               host_name=system('hostname', intern = TRUE)
  # ))
  capabilities<-capabilities[[1]]
  capabilities$mem_kb<-as.numeric(capabilities$mem_kb)
  capabilities$cores<-as.numeric(capabilities$cores)

  obj<-readBin('/dev/urandom', what=raw(), n=2*1000*1000)
  e<-new.env()
  assign('obj', obj, envir = e)
  capabilities$net_send_speed<-1/(system.time(MyClusterExport(cl, 'obj', envir = e))[[3]]/2000)
  capabilities$net_receive_speed<-1/(system.time(MyClusterEval(cl, obj))[[3]]/2000)
  # capabilities$net_send_speed<-1/(system.time(parallel::clusterExport(cl, 'obj', envir = e))[[3]]/2000)
  # capabilities$net_receive_speed<-1/(system.time(parallel::clusterEvalQ(cl, obj))[[3]]/2000)

  return(capabilities)
}


get_current_load<-function(cl, script_dir, pid, flag_include_top=FALSE) {
  file.ex<-eval(substitute(
    MyClusterEval(cl, file.exists(file.path(script_dir, 'get_peak_mem.sh'))),
#    parallel::clusterEvalQ(cl, file.exists(file.path(script_dir, 'get_peak_mem.sh'))),
    list(script_dir=script_dir)))[[1]]
  i<-1
  if(!is.logical(file.ex)){
    cat(paste0("file.ex: \n", str(file.ex)))
  }

  # while(!file.ex) {
  #   file.ex<-eval(substitute(
  #     parallel::clusterEvalQ(cl, file.exists(file.path(script_dir, 'get_peak_mem.sh'))),
  #     list(script_dir=script_dir)))[[1]]
  #   cat(paste0("Waiting for scripts, it=", i, '\n'))
  #   i<-i+1
  # }
  if(flag_include_top) {
    stats<-eval(substitute(
      MyClusterEval(cl, list(
#      parallel::clusterEvalQ(cl, list(
        cpuload=as.numeric(system("LC_NUMERIC=\"en_GB.UTF-8\"; top -b -d 0.3 -n2 | grep \"Cpu(s)\" 2>/dev/null | awk '{print $2+$4}' | tail -n1", intern = TRUE)),
        free_mem_kb=as.numeric(system("grep MemAvailable /proc/meminfo | awk '{print $2}'", intern = TRUE)),
        mem_kb=as.numeric(system2(file.path(script_dir, 'get_current_mem.sh'),stdout=TRUE)),
        peak_mem_kb=as.numeric(system2(file.path(script_dir, 'get_peak_mem.sh'),stdout=TRUE)),
        cpu_time=as.numeric(system2(file.path(script_dir, 'current_time.sh'), args = pid ,stdout=TRUE)),
        wall_time=as.numeric(Sys.time())
      ))[[1]],
      list(script_dir=script_dir, pid=pid)))
  } else {
    # free_mem_kb<-eval(substitute(
    #   parallel::clusterEvalQ(cl,
    #     as.numeric(system("grep MemAvailable /proc/meminfo | awk '{print $2}'", intern = TRUE)))[[1]],
    #   list(script_dir=script_dir, pid=pid)))
    # if(!is.numeric(free_mem_kb)) {
    #   free_mem_kb<-eval(substitute(
    #     parallel::clusterEvalQ(cl,
    #                            as.numeric(system("grep MemAvailable /proc/meminfo | awk '{print $2}'", intern = TRUE)))[[1]],
    #     list(script_dir=script_dir, pid=pid)))
    # }
    # if(!is.numeric(free_mem_kb)) browser()
    #
    # mem_kb<-eval(substitute(
    #   parallel::clusterEvalQ(cl,
    #     as.numeric(system2(file.path(script_dir, 'get_current_mem.sh'),stdout=TRUE)))[[1]],
    #   list(script_dir=script_dir, pid=pid)))
    # if(!is.numeric(mem_kb)) browser()
    #
    # peak_mem_kb<-eval(substitute(
    #   parallel::clusterEvalQ(cl,
    #     as.numeric(system2(file.path(script_dir, 'get_peak_mem.sh'),stdout=TRUE)))[[1]],
    #   list(script_dir=script_dir, pid=pid)))
    # if(!is.numeric(peak_mem_kb)) browser()
    #
    # cpu_time<-eval(substitute(
    #   parallel::clusterEvalQ(cl,
    #     as.numeric(system2(file.path(script_dir, 'current_time.sh'), args = pid ,stdout=TRUE)))[[1]],
    #   list(script_dir=script_dir, pid=pid)))
    # if(!is.numeric(cpu_time)) browser()
    #
    # wall_time<-eval(substitute(
    #   parallel::clusterEvalQ(cl,
    #     as.numeric(Sys.time()))[[1]],
    #   list(script_dir=script_dir, pid=pid)))
    # if(!is.numeric(wall_time)) browser()
    #
    # for(i in 1:20) {
    #   wall_time<-eval(substitute(
    #     parallel::clusterEvalQ(cl,
    #                            as.numeric(Sys.time()))[[1]],
    #     list(script_dir=script_dir, pid=pid)))
    #   if(!is.numeric(wall_time)) browser()
    # }
    #
    # stats<-list(
    #   free_mem_kb=free_mem_kb,
    #   mem_kb=mem_kb,
    #   peak_mem_kb=peak_mem_kb,
    #   cpu_time=cpu_time,
    #   wall_time=wall_time)

    stats<-eval(substitute(
      MyClusterEval(cl, list(
#      parallel::clusterEvalQ(cl, list(
        free_mem_kb=as.numeric(system("grep MemAvailable /proc/meminfo | awk '{print $2}'", intern = TRUE)),
        mem_kb=as.numeric(system2(file.path(script_dir, 'get_current_mem.sh'),stdout=TRUE)),
        peak_mem_kb=as.numeric(system2(file.path(script_dir, 'get_peak_mem.sh'),stdout=TRUE)),
        cpu_time=as.numeric(system2(file.path(script_dir, 'current_time.sh'), args = pid ,stdout=TRUE)),
        wall_time=as.numeric(Sys.time())
      ))[[1]],
      list(script_dir=script_dir, pid=pid)))
  }
  if(!'list' %in% class(stats)) {
    stats <- get_current_load(cl, script_dir, pid, flag_include_top)
  }
  return(stats)
}

benchmark_speed_compression<-function(cl, obj_size=1000) {
  #First we prepare a large typical object
  eval(substitute(MyClusterEval(cl,{df<-data.frame(a=runif(obj_size));1}), list(obj_size=obj_size)))
#  eval(substitute(parallel::clusterEvalQ(cl,{df<-data.frame(a=runif(obj_size));1}), list(obj_size=obj_size)))
  speeds<-list()
  speeds$xz<-system.time(a<-receive_big_object(cl, 'df', 'xz'))[[3]]
  speeds$bzip2<-system.time(a<-receive_big_object(cl, 'df', 'bzip2'))[[3]]
  speeds$gzip<-system.time(a<-receive_big_object(cl, 'df', 'gzip'))[[3]]
  speeds$none<-system.time(a<-receive_big_object(cl, 'df', 'none'))[[3]]
  speeds$raw<-system.time(a<-receive_big_object(cl, 'df', ''))[[3]]
  speeds$objsize<-MyClusterEval(cl, object.size(df))
#  speeds$objsize<-parallel::clusterEvalQ(cl, object.size(df))
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
            mem_kb=load_after$mem_kb,
            peak_mem_kb=load_after$peak_mem_kb,
            peak_mem_kb_delta=load_after$peak_mem_kb-load_before$mem_kb,
            mem_kb_delta=load_after$mem_kb - load_before$mem_kb,
            cpu_time=load_after$cpu_time - load_before$cpu_time,
            free_mem_kb=load_after$free_mem_kb,
            wall_time=load_after$wall_time - load_before$wall_time)
  return(ans)
}
