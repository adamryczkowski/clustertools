futile.logger::flog.threshold(futile.logger::ERROR)
futile.logger::flog.remove('mutex.unlock')
futile.logger::flog.remove('mutex.lock')
list_of_hosts<-list("192.168.10.134",
#                    "10.55.181.54",
                    'localhost')


get_first_host<-function(port=11011) {
  for(ip in list_of_hosts) {
    if(can_connect_to_host(ip, paste0("localhost:",port))=="") {
      return(ip)
    }
  }
  return("")
}
remote_host<-get_first_host()

#can_connect_to_host("192.168.10.134", "localhost:11011")
#can_connect_to_host("localhost", "localhost:11011")
