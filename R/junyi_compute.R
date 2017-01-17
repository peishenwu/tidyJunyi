#' A workable alternative for dplyr::compute to use in junyi bigquery datasets
#' @description the dplyr's compute() function will not work due to bigquery not supporting CREATE TABLE methods, use this alternative instead
#' @param x a tbl object to be computed, the results will be saved into a temporary dataset
#' @param tablename if not specified, then a random name will be generated
#' @return returns dplyr tbl object
#' @export

junyi.compute <- function(x, tablename = NULL){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]

  ##
  sql <- extract_and_modify_sql(x)

  if(length(tablename)==0){
    tablename <- get_rand_tablename(destination.dataset)
  }#end if

  results <- query_exec(sql, "junyiacademy", max_pages = 1, page_size = 0, warn = F,
                        destination_table = paste(destination.dataset,".",tablename,sep=""),
                        write_disposition = "WRITE_TRUNCATE") #overwrite

  message(paste("temporary table created at ",destination.dataset,".",tablename,sep=""))

  DB <- src_bigquery("junyiacademy", destination.dataset)
  return(tbl(DB, tablename))

}#end function


