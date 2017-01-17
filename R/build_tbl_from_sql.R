#' Build a tbl object from defined SQL query
#' @description This function allows manually build tbl objects from input SQL commands
#' @param sql The SQL command to be executed, the resulted will be saved into a temporary table
#' @param tablename if not specified, a random name will be used
#' @return returns dplyr tbl object
#' @export

build_tbl_from_sql <- function(sql, tablename = NULL){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]

  ##
  if(length(tablename)==0){
    tablename <- get_rand_tablename(destination.dataset) #random non-repeat name
  }#end if

  return(write_bq_dataset(destination.dataset,
                          tablename, force = T, sql))

}#end function
