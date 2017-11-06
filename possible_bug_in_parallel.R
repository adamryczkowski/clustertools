spawn_fn<-function(nr) {
  env<-new.env()
  env$a<-nr
  j<-eval(quote(parallel::mcparallel(a)), envir = env)
  return(list(job=j, value=nr))
}

test_fn<-function(item) {
  ans<-parallel::mccollect(jobs = item$job)[[1]]
  if(length(ans)==0) {
    browser()
  }
  if(item$value!=ans) {
    stop("Error")
  }
}
a<-spawn_fn(1)
test_fn(a)

jobs<-rep(x = list(list()),200)

for(i in seq(10000)) {
  job_nr<-sample.int(length(jobs), 1)
  if(length(jobs[[job_nr]])!=0){
    test_fn(jobs[[job_nr]])
  }
  jobs[[job_nr]]<-spawn_fn(runif(1))
}
