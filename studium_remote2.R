library(parallel)
j1<-parallel::mcparallel(Sys.sleep(5))
j2<-parallel::mcparallel(Sys.sleep(300))

parallel::mccollect(j1)
j2

cl<-parallel::makeCluster('localhost', homogeneous=FALSE)

j1<-parallel::mcparallel(parallel::clusterEvalQ(cl, Sys.sleep(5)))
j2<-parallel::mcparallel(parallel::clusterEvalQ(cl, Sys.sleep(300)))
parallel::mccollect(j1)
