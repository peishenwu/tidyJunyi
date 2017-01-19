#' create tbl objects of various junyi bigquery datasets
#' @description  log data of mission assignment, the minimal time unit for each row is every 10 mins. The primary key for each mission is missionID
#' @param force create a new data table despite a previous table with a same name already exist
#' @param load.only set to true to directly load pre-built tbl without building
#' @return returns dplyr tbl object
#' @export

tbl_log_AssignMission <- function(force = F, load.only = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name (a-z, lower case, 10 characters)
  tablename = "logasnmssn"
  #
  assign.mission.q1 <- "SELECT assigner_id AS from_userID, mission_id AS missionID, assignee_id AS to_userID,
                               DATE(DATE_ADD(timestamp, 8, 'HOUR')) AS date,
                               MONTH(DATE_ADD(timestamp, 8, 'HOUR')) AS month,
                               DAYOFWEEK(DATE_ADD(timestamp, 8, 'HOUR')) AS weekday, WEEK(timestamp) AS week_of_year,
                               HOUR(DATE_ADD(timestamp, 8, 'HOUR')) AS hour,
                               MINUTE(DATE_ADD(timestamp, 8, 'HOUR')) AS minute
                        FROM log_from_gcs.log_assign_mission"

  assign.mission.q2 <- "SELECT assigner_id AS from_userID, mission_id AS missionID, assignee_id AS to_userID,
                               DATE(DATE_ADD(timestamp, 8, 'HOUR')) AS date,
                               MONTH(DATE_ADD(timestamp, 8, 'HOUR')) AS month,
                               DAYOFWEEK(DATE_ADD(timestamp, 8, 'HOUR')) AS weekday, WEEK(timestamp) AS week_of_year,
                               HOUR(DATE_ADD(timestamp, 8, 'HOUR')) AS hour,
                               MINUTE(DATE_ADD(timestamp, 8, 'HOUR')) AS minute
                        FROM streaming_log.log_assign_mission"

  assign.mission.q1 <- paste("SELECT from_userID, missionID, to_userID, date, month, week_of_year, hour,
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
                              FROM (",assign.mission.q1,")",sep="")

  assign.mission.q2 <- paste("SELECT from_userID, missionID, to_userID, date, month, week_of_year, hour,
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
                             FROM (",assign.mission.q2,")",sep="")

  assign.mission.q1 <- paste("SELECT from_userID, missionID, to_userID, date, month, weekday, week_of_year, hour, ten_min_period FROM (",assign.mission.q1,")",sep="")
  assign.mission.q2 <- paste("SELECT from_userID, missionID, to_userID, date, month, weekday, week_of_year, hour, ten_min_period FROM (",assign.mission.q2,")",sep="")
  assign.mission.q <- bqunion(c(assign.mission.q1,assign.mission.q2))

  #transform userID into user_primary_key
  assign.mission.q <- Ijoin(assign.mission.q, user_connector_tbl(dataset.date,
                                                                 user_primary_key = "from_user_primary_key",
                                                                 userID = "from_userID",use.userID = T),"from_userID")
  assign.mission.q <- Ijoin(assign.mission.q, user_connector_tbl(dataset.date,
                                                                 user_primary_key = "to_user_primary_key",
                                                                 userID = "to_userID",use.userID = T),"to_userID")
  #
  assign.mission.q <- paste("SELECT from_user_primary_key, missionID, to_user_primary_key, date, month, weekday, week_of_year, hour, ten_min_period
                             FROM (",assign.mission.q,")",sep="")

  assign.mission.q <- unique.query(assign.mission.q)
  ##
  return(write_bq_dataset(destination.dataset, tablename, force, assign.mission.q, bypass = load.only))

}#end function





