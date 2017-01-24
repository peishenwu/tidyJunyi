#' load tbl by tablename
#' @param tablename the table name to be loaded
#' @param dataset.name the dataset name, if not given, then the dataset parameters must be defined by set_dataset()
#' @return returns dplyr tbl object
#' @export
#'

load_tbl_by_name <- function(tablename, dataset.name = NULL){

  if(length(dataset.name)==0){
    ##use global variable
    tidyJunyi.settings <- get("tidyJunyi.settings")
    destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  }else{
    destination.dataset <- dataset.name
  }#end if

  #
  message(paste("loading table: ",tablename," from dataset: ",destination.dataset,sep=""))
  DB <- src_bigquery("junyiacademy", destination.dataset)
  return(tbl(DB, tablename))

}#end function
