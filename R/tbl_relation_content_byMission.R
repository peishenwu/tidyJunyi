#' create tbl objects of various junyi bigquery datasets
#' @description  relationships between contents and missions. The primary key for each mission is missionID, for each content is contentID
#' @param force create a new data table despite a previous table with a same name already exist
#' @return returns dplyr tbl object
#' @export

tbl_relation_content_byMission <- function(force = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]

  #output table name (a-z, lower case, 10 characters)
  tablename = "relmsncont"
  #
  mission.q1 <- "SELECT mission_id AS missionID,
                        REGEXP_REPLACE(split(LTRIM(RTRIM(task_id_list,']'),'['),','),'\"|\\'| ','') AS contentID
                 FROM log_from_gcs.log_assign_mission"

  mission.q2 <- "SELECT mission_id AS missionID,
                        REGEXP_REPLACE(split(LTRIM(RTRIM(task_id_list,']'),'['),','),'\"|\\'| ','') AS contentID
                 FROM streaming_log.log_assign_mission"

  mission.q1 <- paste("SELECT missionID, contentID FROM (",mission.q1,")",sep="")
  mission.q2 <- paste("SELECT missionID, contentID FROM (",mission.q2,")",sep="")
  mission.q <- bqunion(c(mission.q1, mission.q2))

  mission.q <- unique.query(mission.q)

  ##
  return(write_bq_dataset(destination.dataset, tablename, force, mission.q))

}#end function
