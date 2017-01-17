#' create tbl objects of various junyi bigquery datasets
#' @description  relationships between problem(exercise) and it's sub quizes
#' @param force create a new data table despite a previous table with a same name already exist
#' @return returns dplyr tbl object
#' @export

tbl_relation_prob_quiz <- function(force = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name (a-z, lower case, 10 characters)
  tablename = "relprobquz"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)
  #
  q.prob <- paste("SELECT exercise AS contentID, quiz_pid AS quizID
                   FROM junyi_",dataset.date,".ProblemLog_",dataset.date," GROUP BY contentID, quizID",sep="")

  ##count the number of quizes
  quiz.count <- paste("SELECT contentID, EXACT_COUNT_DISTINCT(quizID) AS quiz_count FROM (",q.prob,") GROUP BY contentID",sep="")
  q.prob <- Ljoin(q.prob, quiz.count, "contentID")

  return(write_bq_dataset(destination.dataset, tablename, force, q.prob))

}#end function
