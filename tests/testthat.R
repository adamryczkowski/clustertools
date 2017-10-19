library(testthat)
library(clustertools)
futile.logger::flog.threshold(futile.logger::ERROR)
futile.logger::flog.remove('mutex.unlock')
futile.logger::flog.remove('mutex.lock')

test_check("clustertools")
