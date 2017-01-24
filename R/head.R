#' helper functions that make tbl objects like data.frames
#' @description  an alternative head which works for tbl objects
#' @param x see ?head for details, this can also be tbl objects
#' @export

head <- function(x, ...){
  if(sum("tbl_bigquery" == class(x))!=0){
    view(x, func = "head")
  }else{
    utils::head(x, ...)
  }#end if
}#end function

