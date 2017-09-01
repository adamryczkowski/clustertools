
RemoteServer<-R6::R6Class("RemoteServer",
  public = list(
    initialize=function(host_address, username=NULL,port=11001) {
      private$host_address_<-host_address
      private$job_ <- BackgroundTask$new()

      if(is.null(username)) {
        username<-system('whoami', intern = TRUE)
      }
      default_if<-find_default_if()
      myip=system(paste0("ip addr show ", default_if, " | awk '$1 == \"inet\" {gsub(/\\/.*$/, \"\", $2); print $2}'"), intern=TRUE)


      cl <- parallel::makeCluster(host_address, user=username, master=myip, port=port, homogeneous=FALSE)
      private$job_$run_task(
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
            tmpfile_txt
          })
          parallel::stopCluster(cl)
          c(a, perfscript_remote_path=perfscript_remote_path)
        })
      private$cl_connection_ <- parallel::makeCluster(host_address, user=username, master=myip, port=port, homogeneous=FALSE)
      private$cl_pid_ <- parallel::clusterEvalQ(private$cl_connection_, Sys.getpid())
      private$cl_aux_connection_ <- parallel::makeCluster(host_address, user=username, master=myip, port=port, homogeneous=FALSE)
    },

    finalize=function() {
      parallel::stopCluster(private$cl_connection_)
    },

    print=function() {
      rap<-paste0("Remote host ", self$host_address, " with ",
                  self$cpu_cores, " core CPU and ",
                  utils:::format.object_size(self$mem_size, "auto"), " RAM.\n",
                  "CPU speed: ", utils:::format.object_size(self$cpu_speed*1000000, "auto"), "/second\n",
                  "net_send_speed: ", utils:::format.object_size(self$net_send_speed*1000, "auto"), "/second\n",
                  "net_receive_speed: ", utils:::format.object_size(self$net_receive_speed*1000, "auto"), "/second\n\n"
      )
      cat(rap)
      current_load<-get_current_load(private$cl_aux_connection_)
      rap<-paste0("current CPU utilization: ", current_load$cpuload, "%\n",
                  "free memory: ", utils:::format.object_size(current_load$memkb*1024, "auto"), "\n")
      cat(rep)
    },
    get_aux_connection=function() {private$cl_aux_connection_},
    get_job=function() {private$job},
    get_pid=function() {private$cl_pid_},
    spawn_job=function(remote_expr, export_values) {
      if(!private$job_$is_job_running()){
        private$job_$run_task(expr)
      } else {
        stop("A background job is already running!")
      }
      if(! 'list' %in%  class(export_values)) {
        stop("export_values should be a list")
      }
      private$job_$run_task({

        send_big_object(private$cl_connection_, object = export_values, remote_name = '.tmp.export')
        parallel::clusterExport(private$cl_connection_, varlist = names(export_values), envir = export_values)
        ans<-parallel::clusterEvalQ(private$cl_connection_, remote_expr)
        eval(substitute(parallel::clusterEvalQ(private$cl_connection_, rm(n)), list(n=names(export_values))))

        a=get_cpu_capabilies(cl)
        parallel::stopCluster(cl)
        return(a)
        })

    },
    call_function=function(function_name, arg_list) {

    },
    is_job_running=function(expr) {
      private$job_$is_job_running()
    },
    send_objects=function(named_list_of_objects, flag_wait=FALSE) {
      private$job_$wait_for_task_finish()
      if(!'list' %in% class(named_list_of_objects)) {
        stop("named_list_of_objects must be a named list of objects to upload")
      }
      named_list_of_objects<-named_list_of_objects
      private$job_$run_task(
        send_big_objects(private$cl_connection_, objects = named_list_of_objects)
        )
      if(flag_wait) {
        private$job_$wait_for_task_finish()
      }
    },
    send_file=function(local_path, remote_path, flag_wait=FALSE, flag_check_first=TRUE) {
      private$job_$wait_for_task_finish()
      private$job_$run_task(
        send_file(private$cl_connection_, file_path = local_path, remote_path = remote_path,
                  flag_check_first=flag_check_first)
        )
      if(flag_wait) {
        private$job_$wait_for_task_finish()
      }
    }
  ),


  active = list(
    host_address      = function() {private$fill_capabilities(TRUE); private$host_address_},
    cpu_cores         = function() {private$fill_capabilities(TRUE); private$cpu_cores_},
    perfscript_remote_path = function() {private$fill_capabilities(TRUE); private$perfscript_remote_path_},
    cpu_speed         = function() {private$fill_capabilities(TRUE); private$cpu_speed_},
    mem_size          = function() {private$fill_capabilities(TRUE); private$mem_size_},
    net_send_speed    = function() {private$fill_capabilities(TRUE); private$net_send_speed_},
    net_receive_speed = function() {private$fill_capabilities(TRUE); private$net_receive_speed_},
    cl_connection     = function() {private$cl_connection_},
    cl_aux_connection     = function() {private$cl_aux_connection_},
    job = function() {private$job_}
  ),
  private = list(
    cl_aux_connection_=NA,
    cl_connection_=NA,
    cl_pid_=NA,
    perfscript_remote_path_=NA,
    host_address_=NA,
    net_send_speed_=NA,
    net_receive_speed_=NA,
    cpu_speed_=NA,
    cpu_cores_=NA,
    mem_size_=NA,
    job_=NA,
    fill_capabilities=function(flag_wait) {
      if(!is.na(private$cpu_cores_)) {
        return()
      }
      if(!flag_wait && private$job_$is_task_running()) {
        return()
      }

      capabilities<-private$job_$get_task_return_value()
      private$cpu_cores_<-capabilities$cores
      private$cpu_speed_<-capabilities$speed
      private$mem_size_<-capabilities$memkb * 1024
      private$net_send_speed_<-capabilities$net_send_speed
      private$net_receive_speed_<-capabilities$net_receive_speed
      private$perfscript_remote_path_<-capabilities$perfscript_remote_path
    }
  ),
  cloneable = FALSE
)

# srv1<-RemoteServer$new("rstudio")
# b1=srv1$job
# srv1$cpu_cores
# srv2<-RemoteServer$new('10.29.153.100')





