find_default_if<-function(){
  return(system("/sbin/ip route | awk '/default/ { print $5 }'", intern=TRUE))
}

get_cpu_capabilies<-function(cl) {
  capabilities<-parallel::clusterEvalQ(cl, list(memkb=system("grep MemTotal /proc/meminfo | awk '{print $2}'", intern = TRUE),
                                                cores=system('grep "^core id" /proc/cpuinfo | sort -u | wc -l', intern=TRUE),
                                                speed=1/system.time(system('dd if=/dev/zero bs=3 count=1000000 2>/dev/null  | md5sum  >/dev/null'))[[3]]
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

get_current_load<-function(cl) {
  stats<-parallel::clusterEvalQ(cl, list(
    cpuload=as.numeric(system("top -b -n2 | grep \"Cpu(s)\" 2>/dev/null | awk '{print $2+$4}' | tail -n1", intern = TRUE)),
    memkb=as.numeric(system("grep MemAvailable /proc/meminfo | awk '{print $2}'", intern = TRUE))
  ))[[1]]
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



