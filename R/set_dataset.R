#' set a global destination dataset for temporary tables
#' @param backup.dataset.date weekly backup dataset date, the latest date of the dataset junyi_yymmdd on bigquery, eg. eg. "2017-1-9"
#' @param destination.dataset.name a temporary dataset in which user has access to read and write tables
#' @export
#'

set_dataset <- function(backup.dataset.date, destination.dataset.name){

#   tidyJunyi.settings <<- list(destination.dataset = destination.dataset.name,
#                               backup.dataset = backup.dataset.date)

  assign("tidyJunyi.settings", list(destination.dataset = destination.dataset.name,
                                    backup.dataset = backup.dataset.date),
         envir = .GlobalEnv)

}#end function
