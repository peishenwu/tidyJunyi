#' helper functions that make tbl objects like data.frames
#' @description  an alternative base::table which works for tbl objects
#' @param x see ?table for details, this can also be tbl objects
#' @param column the column containing values to be calculated
#' @param n how many items to show, by default is 5000.
#' @export

table <- function(x, column, n = 5000){

  ##use global variable
  #tidyJunyi.settings <- get("tidyJunyi.settings")
  #destination.dataset <- tidyJunyi.settings[['destination.dataset']]

  ##support for NSE
  parameters <- as.list(match.call())
  column <- as.character(parameters[['column']])
  ##
  if(sum("tbl_bigquery" == class(x))!=0){
    sql <- extract_and_modify_sql(x)
    sql <- paste("SELECT value, freq FROM(
                         SELECT value, COUNT(*) OVER (PARTITION BY value) AS freq FROM (
                                SELECT ",column," AS value FROM (",sql,")))
                  GROUP BY value, freq ORDER BY freq DESC",sep="")

    res <- nrow(sql)
    if(res>5000 & n == 5000){message(paste("response has ",res," rows, only top 5000 rows are shown, use n = Inf to retrieve all",sep = ""))}

    sql <- paste("SELECT * FROM (",sql,") ",ifelse(n == Inf,"",paste("LIMIT ",n,sep="")),sep="")
    ##
    results <- query_exec(sql, "junyiacademy",
                          max_pages = ifelse(n == Inf, Inf, 1),
                          page_size = ifelse(n == Inf, 10000, n),
                          warn = F)

    return(results)
  }else{
    return(base::table(x))
  }#end if
}#end function


