MyClusterEval<-function(cl, expr){
#  expr<-substitute(expr)
#  str_expr<-deparse(expr, width.cutoff = 500)
#  cat(paste0("Eval on ", as.integer(cl[[1]]$con), ": ", str_abbreviate(str_expr, 120),'\n'))
  eval(substitute(parallel::clusterEvalQ(cl, expr), list(expr=expr)))
}

MyClusterExport<-function(cl, varlist, envir){
#  cat(paste0("Export on ", as.integer(cl[[1]]$con), ", variables: ", str_abbreviate(paste(varlist, collapse = ' ') , 60)))
  eval(substitute(parallel::clusterExport( cl, varlist, envir), list(varlist=varlist, envir=envir)))

}

str_abbreviate<-function(str, max, suffix=NULL){
  str<-paste0(str, collapse = '\n')
  str<-stringr::str_trim(str, side='both')
  str<-stringr::str_replace(str, "^[\\r\\n\\t ]*","")
  str<-stringr::str_replace(str, "[\\r\\n\\t ]$","")
  if(nchar(str)<=max) {
    return(str)
  }

  abbr <- ''
  str <- stringr::str_split(str, stringr::fixed(' '))[[1]]
  if(is.null(suffix)) {
    suffix <- "..."
  }
  max <- max - nchar(suffix)


  for(i in seq(1, length(str)))  {
    if(nchar(abbr) + nchar(str[[i]]) < max) {
      abbr <- paste0(abbr, str[[i]], ' ')
    } else { break }
  }
  return(paste0(stringr::str_trim(abbr, side='left'), suffix))
}
