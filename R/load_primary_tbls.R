#' load all pre-built tables
#' @param backup.dataset.date the latest date of the dataset junyi_yymmdd on bigquery. If not specified, then the most recent previous Monday's date will be used.
#' @param source.dataset the dataset where the pre-built tidy tbls are located
#' @param tmp.dataset a temporary dataset in which user has access to read and write tables
#' @param info load info tables
#' @param log load log tables
#' @param relation load relation tables
#' @return returns dplyr tbl object
#' @export

load_primary_tbls <- function(backup.dataset.date = NULL,
                              source.dataset = "tidy_data",
                              tmp.dataset = "tidy_tmp",
                              info = T, log = T, relation = T){
  ##
  base_monday = as.Date("2016-1-4")
  recent_monday <- base_monday + (as.numeric(Sys.Date() - base_monday) %/% 7)*7

  message(paste("loading tables from source dataset: ",source.dataset,sep=""))

  set_dataset(backup.dataset.date = ifelse(length(backup.dataset.date)!=0,
                                           backup.dataset.date,
                                           as.character(recent_monday)),
              destination.dataset.name = source.dataset)
  #
  build_primary_tbls(load.only = T, info=info, log=log, relation = relation) #load tables

  ##
  message(paste("setting temporary dataset to: ",tmp.dataset,sep=""))
  set_dataset(backup.dataset.date = ifelse(length(backup.dataset.date)!=0,
                                           backup.dataset.date,
                                           as.character(recent_monday)),
              destination.dataset.name = tmp.dataset)

}#end if
