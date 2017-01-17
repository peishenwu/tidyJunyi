#' creates a tbl containing various user identifiers
#' @description  the primary key of this table is user_primary_key, however, there exist multiple variables describing such user
#' @param force create a new data table despite a previous table with a same name already exist
#' @return returns dplyr tbl object
#' @export

tbl_relation_user_identifiers <- function(force = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name (a-z, lower case, 10 characters)
  tablename = "relusriden"

  sql <- user_connector_tbl(dataset.date,
                            use.keyID = T, use.uEmail = T,
                            use.cuEmail = T, use.userID = T)
  sql <- unique.query(sql)
  return(write_bq_dataset(destination.dataset, tablename, force, sql))

}#end function
