library(parallel)
fn1<-function(expr) {
  j<-mcparallel(expr = expr)
  return(mccollect(j))
}

fn2<-function(expr) {
  e<-substitute(expr)
  cat(deparse(e))
  cat('\n')
  fn1(expr)
}

fn3<-function(){
  a<-23
  fn2(a+1)
}

fn3()

Obj<-R6::R6Class(
  "Obj",
  public=list(
    initialize=function() {
      eval(substitute(fn2(a+1), list(a=private$a)))
    }
  ),
  private=list(
    a=23
  )
)

obj<-Obj$new()
