#' compute SQL command and insert a column into a tbl object
#' @description can manually insert sql command in which is not supported by dplyr, such as bigquery's window functions
#' @references \url{https://cloud.google.com/bigquery/docs/reference/legacy-sql#windowfunctions}
#' @references \url{https://cran.rstudio.com/web/packages/dplyr/vignettes/window-functions.html}
#' @param x tbl object
#' @param sql sql command to be executed
#' @examples
#' \dontrun{
#' mutate_sql("LAG(date,1) OVER (PARTITION BY user_primary_key ORDER BY date) AS lag_date")
#' }
#' @import bigrquery
#' @import stringr
#' @import dplyr
#' @export

mutate_sql <- function(x, sql){

  ##use global variable
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]

  x.columns <- view(x, func = "colnames") #get col name
  x.columns <- c(x.columns, sql)

  x.sql <- extract_and_modify_sql(x)
  x.sql <- paste("SELECT ",paste(x.columns,collapse=","),"
                FROM (",x.sql,") ",sep="")

  return(build_tbl_from_sql(x.sql))

}#end function

