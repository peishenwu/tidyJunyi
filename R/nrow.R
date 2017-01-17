#' helper functions that make tbl objects like data.frames
#' @description  an alternative nrow which works for tbl objects
#' @param x can be a tbl object, data frame or SQL statement
#' @export

nrow <- function(x){
  if(sum("tbl_bigquery" == class(x))!=0 | sum("character" == class(x))!=0){

    ##if tbl object
    if(sum("tbl_bigquery" == class(x))!=0){sql <- extract_and_modify_sql(x)}
    ##if SQL statement
    if(sum("character" == class(x))!=0){sql <- x}

    #
    sql <- paste("SELECT count(*) AS nrow FROM (",sql,")",sep="")
    results <- query_exec(sql, "junyiacademy",
                          max_pages = 1,
                          page_size = 10000,
                          warn = F)
    return(results$nrow)
  }else{
    return(base::nrow(x))
  }#end if
}#end function


