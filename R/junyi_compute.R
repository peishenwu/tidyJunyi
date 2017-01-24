#' A workable alternative for dplyr::compute to use in junyi bigquery datasets
#' @description the dplyr's compute() function will not work due to bigquery not supporting CREATE TABLE methods, use this alternative instead
#' @param x a tbl object to be computed
#' @param tablename if not specified, then a random name will be generated
#' @param dataset.name the dataset name, if not given, then the dataset parameters must be defined by set_dataset()
#' @return returns dplyr tbl object
#' @export

junyi.compute <- function(x, tablename = NULL, dataset.name = NULL){

  if(length(dataset.name)==0){
    ##use global variable
    tidyJunyi.settings <- get("tidyJunyi.settings")
    destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  }else{
    destination.dataset <- dataset.name
  }#end if

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


