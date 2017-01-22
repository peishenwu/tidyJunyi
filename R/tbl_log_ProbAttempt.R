#' create tbl objects of various junyi bigquery datasets
#' @description  detailed log data of problem(exercise) usage, the unit of each row is each "attempt" of {eventID, quizID} key
#' @param force create a new data table despite a previous table with a same name already exist
#' @param load.only set to true to directly load pre-built tbl without building
#' @return returns dplyr tbl object
#' @export

tbl_log_ProbAttempt <- function(force = F, load.only = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name
  tablename = "Log_ProblemAttempt"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)
  #
  q.prob <- paste("SELECT DATE(DATE_ADD(time_done, 8, 'HOUR')) AS date,
                          DATE_ADD(time_done, 8, 'HOUR') AS timestamp, user.email AS user_primary_key,
                          exercise AS eventID, quiz_pid AS quizID,
                          time_taken AS total_time_consumed, count_attempts AS total_attempt_count,
                          time_taken_attempts AS each_attempt_time_consumed,
                          ROW_NUMBER() OVER (PARTITION BY user.email, exercise, quiz_pid, problem_number
                                             ORDER BY time_done ASC) AS attempt_sequence
                   FROM junyi_",dataset.date,".ProblemLog_",dataset.date,sep="")

  return(write_bq_dataset(destination.dataset, tablename, force, q.prob, bypass = load.only))

}#end function
