#' create tbl objects of various junyi bigquery datasets
#' @description  junyi content's specs data, including categories and type
#' @param force create a new data table despite a previous table with a same name already exist
#' @return returns dplyr tbl object
#' @export

tbl_info_Content <- function(force = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]

  #output table name
  tablename = "infocontnt"

  ##
  max_time.q <- "SELECT MAX(make_default_time) AS make_default_time FROM [FinalTable.ContentFinalInfo]"
  content.q <- paste("SELECT content_name AS contentID,
                             content_pretty_name, content_kind, content_live, level1_name, level2_name, level3_name, level4_name, level5_name, make_default_time
                      FROM [FinalTable.ContentFinalInfo]
                      WHERE make_default_time IN (",max_time.q,")", sep="")

  #check whether table exists, if not then create a new one (unless force is true)
  return(write_bq_dataset(destination.dataset, tablename, force, content.q))

}#end function

