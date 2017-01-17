#' create tbl objects of various junyi bigquery datasets
#' @description  relationships between contents and missions. The primary key for each mission is missionID, for each content is contentID
#' @param force create a new data table despite a previous table with a same name already exist
#' @return returns dplyr tbl object
#' @export

tbl_relation_mission_content <- function(force = F){

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

  mission.q1 <- paste("SELECT missionID, contentID FROM FLATTEN((",mission.q1,"),contentID)",sep="")
  mission.q2 <- paste("SELECT missionID, contentID FROM FLATTEN((",mission.q2,"),contentID)",sep="")
  mission.q <- bqunion(c(mission.q1, mission.q2))
  ##
  max_time.q <- "SELECT MAX(make_default_time) AS make_default_time FROM [FinalTable.ContentFinalInfo]"
  content.q <- paste("SELECT content_name AS contentID,
                             content_pretty_name, content_kind, content_live
                      FROM [FinalTable.ContentFinalInfo]
                      WHERE make_default_time IN (",max_time.q,")", sep="")

  mission.q <- Ljoin(mission.q, content.q, "contentID")

  ##count the mission's video and problems
  mission.video.count <- paste("SELECT missionID, EXACT_COUNT_DISTINCT(contentID) AS video_count
                                FROM (",mission.q,") WHERE content_kind == 'Video' GROUP BY missionID",sep="")
  mission.video.count <- paste("SELECT missionID, video_count FROM (",mission.video.count,")",sep="")
  #
  mission.prob.count <- paste("SELECT missionID, EXACT_COUNT_DISTINCT(contentID) AS prob_count
                               FROM (",mission.q,") WHERE content_kind == 'Exercise' GROUP BY missionID",sep="")
  mission.prob.count <- paste("SELECT missionID, prob_count FROM (",mission.prob.count,")",sep="")
  ##
  mission.q <- Ljoin(mission.q, mission.video.count, "missionID")
  mission.q <- Ljoin(mission.q, mission.prob.count, "missionID")
  ##
  mission.q <- unique.query(mission.q)
  mission.q <- paste("SELECT missionID, contentID, content_pretty_name, content_kind, content_live,
                             IFNULL(INTEGER(video_count),0) AS video_count, IFNULL(INTEGER(prob_count),0) AS prob_count
                      FROM (",mission.q,") ORDER BY missionID")
  ##
  return(write_bq_dataset(destination.dataset, tablename, force, mission.q))

}#end function
