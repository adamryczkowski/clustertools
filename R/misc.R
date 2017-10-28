find_default_if<-function(target_ip=NULL){
  target_ip<-curl::nslookup(target_ip)
  if(target_ip=='127.0.0.1'){
    return('lo')
  }
  if(is.null(target_ip)) {
    return(system("/sbin/ip route | awk '/default/ { print $5 }'", intern=TRUE))
  } else {
    ips<-iptools::hostname_to_ip(target_ip)[[1]]
    routes<-stringr::str_split(system("/sbin/ip route", intern = TRUE), stringr::fixed(" "))
    routes_tab<-sapply(routes, function(x) c(x[[1]], x[[2]]))
    pos<-which(routes_tab[1,]=='default')
    routes_tab<-t(sapply(routes[-pos], function(x) c(x[[1]], x[[3]])))
    pos<-which(iptools::ip_in_range(rep(target_ip, nrow(routes_tab)), routes_tab[,1]))
    if(length(pos)==0) {
      return(system("/sbin/ip route | awk '/default/ { print $5 }'", intern=TRUE))
    } else {
      return(routes_tab[pos,2])
    }
  }
}

ifaddr<-function(idev='lo') {
  system(paste0('/sbin/ip -o -4 addr list ', idev, " | awk '{print $4}' | cut -d/ -f1"), intern = TRUE)
}

get_cpu_capabilies<-function(cl) {
  capabilities<-MyClusterEval(cl, list(mem_kb=system("grep MemTotal /proc/meminfo | awk '{print $2}'", intern = TRUE),
                                                cores=system('lscpu -p=cpu,core,socket | grep -v ^#', intern = TRUE),
                                                speed=1/system.time(system('dd if=/dev/zero bs=3 count=1000000 2>/dev/null  | md5sum  >/dev/null'))[[3]],
                                                speed2=
                                      if(system2("which", args="sysbench", stdout=FALSE)==0) {
                                        as.numeric(system('sysbench --test=cpu --cpu-max-prime=20000 run | grep "total time:" | grep -Eo "[[:digit:].]+"', intern = TRUE))
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
  cpu_matrix<-plyr::laply(purrr::map(capabilities$cores, ~stringr::str_split(.,pattern=stringr::fixed(','))[[1]]), identity)

  capabilities$cores<-length(unique(cpu_matrix[,2]))
  capabilities$cpus<-length(unique(cpu_matrix[,3]))
  capabilities$threads<-length(unique(cpu_matrix[,1]))

  obj<-readBin('/dev/urandom', what=raw(), n=2*1000*1000)
  e<-new.env()
  assign('obj', obj, envir = e)
  capabilities$net_send_speed<-1/(system.time(MyClusterExport(cl, 'obj', envir = e))[[3]]/20000)
  capabilities$net_receive_speed<-1/(system.time(MyClusterEval(cl, obj))[[3]]/20000)
  # capabilities$net_send_speed<-1/(system.time(parallel::clusterExport(cl, 'obj', envir = e))[[3]]/2000)
  # capabilities$net_receive_speed<-1/(system.time(parallel::clusterEvalQ(cl, obj))[[3]]/2000)

  ping_time<-0
  for(i in seq(3)) {
    ping_time<-ping_time+system.time(MyClusterEval(cl, 2+2))[[3]]
  }
  capabilities$ping_time<-ping_time/3

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
        free_mem_kb2=as.numeric(system("grep MemFree /proc/meminfo | awk '{print $2}'", intern = TRUE)),
        mem_kb=as.numeric(system2(file.path(script_dir, 'get_current_mem.sh'),stdout=TRUE)),
        peak_mem_kb=as.numeric(system2(file.path(script_dir, 'get_peak_mem.sh'),stdout=TRUE)),
        cpu_time=as.numeric(system2(file.path(script_dir, 'current_time.sh'), args = pid ,stdout=TRUE)),
        wall_time=as.numeric(Sys.time())
      ))[[1]],
      list(script_dir=script_dir, pid=pid)))
  }
  if(!'list' %in% class(stats)) {
    stats <- get_current_load(cl, script_dir, pid, flag_include_top)
  } else {
    if(length(stats$free_mem_kb)==0) {
      stats$free_mem_kb<-stats$free_mem_kb2
    }
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
  e<-new.env()
  e$make_scripts<-make_scripts
  e$all_scripts<-all_scripts
  parallel::clusterExport(cl, c('make_scripts','all_scripts'), e)
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

can_connect_to_host<-function(remote, master) {
  remote_els<-XML::parseURI(paste0('ssh://', remote))
  ans<-system(command = paste0('ping -c 1 ',remote_els$server, ' -W 1'), ignore.stdout = TRUE, ignore.stderr = TRUE)
  if(ans!=0) {
    ans<-paste0("Host ", remote_els$server, " cannot be reached by ICMP-ECHO (ping)")
    return(ans)
  }
  if(is.na(remote_els$port)){
    remote_els$port<-22
  }
  ans<-system(command = paste0('timeout 2 nc -z ',remote_els$server, ' ', remote_els$port), ignore.stdout = TRUE, ignore.stderr = TRUE)
  if(ans!=0){
    if(ans==125) {
      ans=paste0("Host ", remote, " doesn't seem to respond on TCP port ", remote_els$port)
    } else {
      ans=paste0("Host ", remote, " rejects connections on TCP port ", remote_els$port)
    }
    return(ans)
  }
  sshcmd<-paste0(if(remote_els$user=="") "" else paste0(remote_els$user, "@"), remote_els$server, ' -p ', remote_els$port)
  while (TRUE) {
    ans_txt<-suppressWarnings(system2("ssh", c("-o PasswordAuthentication=no -o BatchMode=yes ", sshcmd, " -- exit"), stderr=TRUE))
    ans<-attr(ans_txt, 'status')

    if(length(ans)>0){
      if(stringr::str_detect(ans_txt, pattern = stringr::fixed("Host key verification failed"))) {
        hostkey<-system(paste0("ssh-keyscan -p ", remote_els$port, ' ', remote_els$server), intern = TRUE, ignore.stderr = TRUE)
        if(hostkey!='') {
          write(hostkey,file="~/.ssh/known_hosts",append=TRUE)
        }
      } else {
        ans=paste0("Cannot non-interactively estabilish SSH connection with ", remote,
                   ". Try connecting manually using ssh ", sshcmd, " and make sure it connects without any prompts.")
        return(ans)
      }
    } else {
      break
    }
  }

  master_els<-XML::parseURI(paste0('ssh://', master))
  if(is.na(master_els$port)){
    if(is.numeric(master_els$server)) {
      master_els$port<-as.numeric(master_els$server)
      master_els$server<-"localhost"
    }
    ans=paste0("You must give a proper port number")
  }
  ans<-system(command = paste0('timeout 2 nc -z localhost ', master_els$port), ignore.stdout = TRUE, ignore.stderr = TRUE)
  if(ans==0){
    ans=paste0("Port ", master_els$port, " on our machine (server) is already open. Please choose free port on master")
    return(ans)
  }

  #Check if the host has netcat
  has_nc<-system(command = paste0('ssh -o PasswordAuthentication=no -o BatchMode=yes ', sshcmd,
                               " -- which nc"), ignore.stdout = TRUE, ignore.stderr = TRUE)==0


  if(has_nc) {
    #Check if the port is closed from point of view of the remote host
    ans<-system(command = paste0('ssh -o PasswordAuthentication=no -o BatchMode=yes ', sshcmd,
                                 " -- timeout 2 nc -z ", master_els$server, " ", master_els$port))
    if(ans!=0){
      #Check the port after we estabilish a connection:
      #1. Open the port on master
      expr<-substitute(socketConnection(host=host, port = port, blocking=FALSE, server=TRUE, open="r+", timeout=3),
                       list(host=master_els$server, port=master_els$port))
      job<-parallel::mcparallel(expr)
      #2. Check if the port is closed from point of view of the remote host
      ans<-system(command = paste0('ssh -o PasswordAuthentication=no -o BatchMode=yes ', sshcmd,
                                   " -- timeout 2 nc -z ", master_els$server, " ", master_els$port))
      if(ans!=0) {
        ans=paste0("Cannot connect to port ", master_els$port, " on ", master_els$port, " seen from the remote ", remote, " doesn't seem to connect with the localhost. Please check port forwarding and be sure to forward this port to local port ", master_els$port)
        return(ans)
      }
      parallel::mccollect(job) #Close the port if it wasn't closed already

    }

    # expr<-substitute(socketConnection(host=host, port = port, blocking=FALSE, server=TRUE, open="r+", timeout=3),
    #                  list(host=master_els$server, port=master_els$port))
    # job<-parallel::mcparallel(expr)
    # #2. Check if the port is closed from point of view of the remote host
    # ans<-system(command = paste0('ssh -o PasswordAuthentication=no -o BatchMode=yes ', sshcmd,
    #                              " -- timeout 2 nc -z ", master_els$server, " ", master_els$port))
    # if(ans!=0){
    #   ans=paste0("Cannot connect to port ", master_els$port, " on ", master_els$port, " seen from the remote ", remote, " doesn't seem to connect with the localhost. Please check port forwarding and be sure to forward this port to local port ", master_els$port)
    #   return(ans)
    # }
    # parallel::mccollect(job) #Close the port if it wasn't closed already
  }


  return("")
}

get_call_stack<-function(nskip=1) {
  n <- length(x <- sys.calls())
  srcfile<-character(n)
  srcline<-character(n)
  srcexpr<-character(n)
  for (i in 1L:n) {
    xi <- x[[i]]
    m <- length(xi)
    if (!is.null(srcref <- attr(xi, "srcref"))) {
      srcfile_tmp <- attr(srcref, "srcfile")
      srcfile[[i]]<-pathcat::make.path.relative(target.path = normalizePath(srcfile_tmp$filename), base.path = getwd())
      srcline[[i]]<-srcref[1L]
    }
    srcexpr[[i]]=paste0(deparse(xi), collapse = "\n")
  }
  return(data.frame(expr=srcexpr[-seq(n-nskip, n)], file=srcfile[-seq(n-nskip, n)], line=as.numeric(srcline[-seq(n-nskip, n)]), stringsAsFactors = FALSE))
}

format_call_stack<-function(cs) {
  valid_lines<-which(!is.na(cs$line))
  if(length(valid_lines)==0) {
    paste0(cs$expr, collapse = "->")
  } else {
    paste0(paste0(cs$expr[valid_lines], " ", cs$file[valid_lines], "#", cs$line[valid_lines]), collapse = "->")
  }
}

futile.logger::flog.threshold(futile.logger::WARN)

get_mutex<-function() {
  trace<-get_call_stack()
#  futile.logger::flog.info("%s: ", str(tail(trace, 3)), name='mutex.new')
  mut_name<-synchronicity::uuid()
  ans <- synchronicity::boost.mutex(mut_name)
  futile.logger::flog.info("PID %s %s got new mutex %s", Sys.getpid(), format_call_stack(trace), mut_name, name='mutex.new')
  return(ans)
}

lock_mutex<-function(m) {
  trace<-get_call_stack()
  a<-deparse(substitute(m))
  futile.logger::flog.info("PID %s %s is trying to lock mutex %s with id %s",
                           Sys.getpid(),
                           format_call_stack(trace),
                           a, synchronicity::describe(m)@description$shared.name, name='mutex.lock')
  synchronicity::lock(m)
  futile.logger::flog.info("PID %s %s Mutex %s (id %s) is locked",
                           Sys.getpid(),
                           format_call_stack(trace),
                           a, synchronicity::describe(m)@description$shared.name, name='mutex.lock')
}

unlock_mutex<-function(m) {
  trace<-get_call_stack()
  a<-deparse(substitute(m))
  synchronicity::unlock(m)
  futile.logger::flog.info("PID %s %s Mutex %s (id %s) is UNlocked",
                           Sys.getpid(),
                           format_call_stack(trace),
                           a, synchronicity::describe(m)@description$shared.name,
                           name='mutex.unlock')
}
