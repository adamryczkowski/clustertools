list_of_hosts<-list("192.168.10.134",
#                    "10.55.181.54",
                    'localhost')


get_first_host<-function() {
  for(ip in list_of_hosts) {
    if(can_connect_to_host(ip)=="") {
      return(ip)
    }
  }
  return("")
}
remote_host<-get_first_host()

futile.logger::flog.threshold(futile.logger::ERROR)
futile.logger::flog.remove('mutex.unlock')
futile.logger::flog.remove('mutex.lock')
