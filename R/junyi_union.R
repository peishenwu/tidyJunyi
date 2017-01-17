#' A workable alternative for dplyr::union to use in junyi bigquery datasets
#' @param ... tbl objects to perform set union
#' @return returns dplyr tbl object
#' @export

junyi.union <- function(...){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]

  #
  param <- as.list(match.call())
  param <- sapply(2:length(param), function(x){extract_and_modify_sql(eval(param[[x]]))})

  tablename <- get_rand_tablename(destination.dataset)
  union.sql <- paste("SELECT * FROM ",paste(paste("(",param,")",sep=""),collapse = ","),sep="")

  results <- query_exec(union.sql, "junyiacademy", max_pages = 1, page_size = 0, warn = F,
                        destination_table = paste(destination.dataset,".",tablename,sep=""),
                        write_disposition = "WRITE_TRUNCATE") #overwrite

  message(paste("temporary table created at ",destination.dataset,".",tablename,sep=""))

  DB <- src_bigquery("junyiacademy", destination.dataset)
  result.tbl <- tbl(DB, tablename)
  return(result.tbl)

}#end function

