#' create tbl objects of various junyi bigquery datasets
#' @description  log data of doing mission contents, the minimal time unit for each row is every 10 mins. The primary key for each mission is missionID, for each content is contentID
#' @param force create a new data table despite a previous table with a same name already exist
#' @return returns dplyr tbl object
#' @import bigrquery
#' @import stringr
#' @import dplyr
#' @export

tbl_log_DoingMission <- function(force = F){

  ##use global variable
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name (a-z, lower case, 10 characters)
  tablename = "logdomissn"
  #
  complete.mission.q1 <- "SELECT user_id AS userID, mission_id AS missionID,	exercise_name AS contentID,
                                 DATE(DATE_ADD(timestamp, 8, 'HOUR')) AS date,
                                 MONTH(DATE_ADD(timestamp, 8, 'HOUR')) AS month,
                                 DAYOFWEEK(DATE_ADD(timestamp, 8, 'HOUR')) AS weekday, WEEK(timestamp) AS week_of_year,
                                 HOUR(DATE_ADD(timestamp, 8, 'HOUR')) AS hour,
                                 MINUTE(DATE_ADD(timestamp, 8, 'HOUR')) AS minute
                          FROM log_from_gcs.log_exercise_mission_completed"

  complete.mission.q2 <- "SELECT user_id AS userID, mission_id AS missionID,	exercise_name AS contentID,
                                 DATE(DATE_ADD(timestamp, 8, 'HOUR')) AS date,
                                 MONTH(DATE_ADD(timestamp, 8, 'HOUR')) AS month,
                                 DAYOFWEEK(DATE_ADD(timestamp, 8, 'HOUR')) AS weekday, WEEK(timestamp) AS week_of_year,
                                 HOUR(DATE_ADD(timestamp, 8, 'HOUR')) AS hour,
                                 MINUTE(DATE_ADD(timestamp, 8, 'HOUR')) AS minute
                          FROM streaming_log.log_exercise_mission_completed"

  complete.mission.q1 <- paste("SELECT userID, missionID, contentID, date, month, week_of_year, hour,
                             (minute - minute%10) AS ten_min_period,
                             CASE
                             WHEN weekday=1 THEN 7
                             WHEN weekday=2 THEN 1
                             WHEN weekday=3 THEN 2
                             WHEN weekday=4 THEN 3
                             WHEN weekday=5 THEN 4
                             WHEN weekday=6 THEN 5
                             WHEN weekday=7 THEN 6
                             END AS weekday
                             FROM (",complete.mission.q1,")",sep="")

  complete.mission.q2 <- paste("SELECT userID, missionID, contentID, date, month, week_of_year, hour,
                             (minute - minute%10) AS ten_min_period,
                             CASE
                             WHEN weekday=1 THEN 7
                             WHEN weekday=2 THEN 1
                             WHEN weekday=3 THEN 2
                             WHEN weekday=4 THEN 3
                             WHEN weekday=5 THEN 4
                             WHEN weekday=6 THEN 5
                             WHEN weekday=7 THEN 6
                             END AS weekday
                             FROM (",complete.mission.q2,")",sep="")

  complete.mission.q1 <- paste("SELECT userID, missionID, contentID, date, month, weekday, week_of_year, hour, ten_min_period FROM (",complete.mission.q1,")",sep="")
  complete.mission.q2 <- paste("SELECT userID, missionID, contentID, date, month, weekday, week_of_year, hour, ten_min_period FROM (",complete.mission.q2,")",sep="")

  complete.mission.q <- bqunion(c(complete.mission.q1, complete.mission.q2))

  #transform userID into user_primary_key
  complete.mission.q <- Ijoin(complete.mission.q, user_connector_tbl(dataset.date, use.userID = T),"userID")
  complete.mission.q <- paste("SELECT user_primary_key, missionID, contentID, date, month, weekday, week_of_year, hour, ten_min_period FROM (",complete.mission.q,")",sep="")
  complete.mission.q <- unique.query(complete.mission.q)

  ##
  return(write_bq_dataset(destination.dataset, tablename, force, complete.mission.q))

}#end function





