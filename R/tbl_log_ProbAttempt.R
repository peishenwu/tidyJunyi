#' create tbl objects of various junyi bigquery datasets
#' @description  detailed log data of problem(exercise) usage, the unit of each row is each "attempt"
#' @param force create a new data table despite a previous table with a same name already exist
#' @return returns dplyr tbl object
#' @export

tbl_log_ProbAttempt <- function(force = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name (a-z, lower case, 10 characters)
  tablename = "logprobatm"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)
  #
  q.prob <- paste("SELECT DATE(DATE_ADD(time_done, 8, 'HOUR')) AS date,
                          DATE_ADD(time_done, 8, 'HOUR') AS timestamp, user.email AS user_primary_key,
                          exercise AS eventID, quiz_pid AS quizID,
                          time_taken AS total_time_consumed, count_attempts AS total_attempt_count,
                          time_taken_attempts AS each_attempt_time_consumed
                   FROM junyi_",dataset.date,".ProblemLog_",dataset.date,sep="")

  return(write_bq_dataset(destination.dataset, tablename, force, q.prob))

}#end function
