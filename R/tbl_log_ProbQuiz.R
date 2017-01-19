#' create tbl objects of various junyi bigquery datasets
#' @description  detailed log data of problem(exercise) usage, the unit of each row is each prob's quiz
#' @param force create a new data table despite a previous table with a same name already exist
#' @param load.only set to true to directly load pre-built tbl without building
#' @return returns dplyr tbl object
#' @export

tbl_log_ProbQuiz <- function(force = F, load.only = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name (a-z, lower case, 10 characters)
  tablename = "logprobquz"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)
  #
  q.prob <- paste("SELECT DATE(DATE_ADD(time_done, 8, 'HOUR')) AS date,
                          DATE_ADD(time_done, 8, 'HOUR') AS timestamp, user.email AS user_primary_key,
                          exercise AS eventID, quiz_pid AS quizID,
                          correct AS is_correct, points_earned, earned_proficiency AS proficiency_status,
                          time_taken AS total_time_consumed, count_attempts AS total_attempt_count,
                          count_hints AS hint_count, hint_used AS is_hint_used,
                          IFNULL(exam_mode,FALSE) AS exam_mode,
                          IFNULL(topic_mode, FALSE) AS topic_mode,
                          IFNULL(review_mode, FALSE) AS review_mode,
                          IFNULL(pretest_mode, FALSE) AS pretest_mode
                   FROM junyi_",dataset.date,".ProblemLog_",dataset.date,"
                   GROUP BY date, timestamp, user_primary_key, eventID, quizID, is_correct, points_earned,
                            proficiency_status, total_time_consumed, total_attempt_count, hint_count, is_hint_used,
                            exam_mode, topic_mode, review_mode, pretest_mode",sep="")

  return(write_bq_dataset(destination.dataset, tablename, force, q.prob, bypass = load.only))

}#end function
