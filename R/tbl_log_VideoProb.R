#' create tbl objects of various junyi bigquery datasets
#' @description  log data of video and problem(exercise) usage, the minimal time unit for each row is every 10 mins.
#' @param force create a new data table despite a previous table with a same name already exist
#' @param window.begin the date to begin watching, in format yy/mm/dd
#' @param window.end the date to end watching, in format yy/mm/dd
#' @return returns dplyr tbl object
#' @export

tbl_log_VideoProb <- function(force = F, window.begin = NULL, window.end = NULL){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name (a-z, lower case, 10 characters)
  tablename = "logvidprob"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)
  #

  q.TANET.IP <- "SELECT IP, 1 AS isTANET FROM (SELECT ip_address AS IP FROM static.new_ip_schools_mapping)"

  #
  q.prob <- paste("SELECT user.email AS user_primary_key, 'prob' AS eventType, ip_address AS IP,
                          points_earned, earned_proficiency AS proficiency_status, 0 AS video_complete_status, time_taken AS time_consumed,
                          exercise AS eventID, DATE_ADD(time_done, 8, 'HOUR') AS timestamp
                   FROM junyi_",dataset.date,".ProblemLog_",dataset.date,sep="")

  q.video <- paste("SELECT user.email AS user_primary_key, 'video' AS eventType, ip_address AS IP,
                           points_earned, is_video_completed AS video_complete_status, 0 AS proficiency_status, seconds_watched AS time_consumed,
                           youtube_id AS eventID, DATE_ADD(time_watched, 8, 'HOUR') AS timestamp
                    FROM junyi_",dataset.date,".VideoLog_",dataset.date,sep="")
  ##
  q.total <- paste("SELECT user_primary_key, points_earned, proficiency_status, video_complete_status, eventID, eventType, IP,
                           DATE(DATE_ADD(timestamp, 8, 'HOUR')) AS date, timestamp, time_consumed,
                           WEEK(timestamp) AS week_of_year, YEAR(timestamp) AS year,
                           MONTH(DATE_ADD(timestamp, 8, 'HOUR')) AS month,
                           DAYOFWEEK(DATE_ADD(timestamp, 8, 'HOUR')) AS weekday,
                           HOUR(DATE_ADD(timestamp, 8, 'HOUR')) AS hour,
                           MINUTE(DATE_ADD(timestamp, 8, 'HOUR')) AS minute
                    FROM (",q.video,"),(",q.prob,")",sep="")

  ##perform window selection
  window.cond = c()
  if(length(window.begin)!=0){window.cond <- c(window.cond, paste("timestamp >= timestamp('",window.begin,"')",sep=""))}
  if(length(window.end)!=0){window.cond <- c(window.cond, paste("timestamp <= timestamp('",window.end,"')",sep=""))}

  if(length(window.cond)!=0){
    q.total <- paste("SELECT * FROM (",q.total,") WHERE ",paste(window.cond,collapse = " AND "),sep="")
  }#end if

  #convert bigquery's 1 (Sunday) and 7 (Saturday) into 1 (Monday) and 7 (Sunday)
  q.total <- paste("SELECT user_primary_key, points_earned, proficiency_status, video_complete_status, eventID, eventType, date, IP,
                           time_consumed, week_of_year, year, month, hour,
                           (minute - minute%10) AS ten_min_period,
                           CASE
                                WHEN weekday=1 THEN 7
                                WHEN weekday=2 THEN 1
                                WHEN weekday=3 THEN 2
                                WHEN weekday=4 THEN 3
                                WHEN weekday=5 THEN 4
                                WHEN weekday=6 THEN 5
                                WHEN weekday=7 THEN 6
                           END AS weekday,
                           CASE
                                WHEN month=1  THEN 'semester1'
                                WHEN month=2  THEN 'winter_vacation'
                                WHEN month=3  THEN 'semester2'
                                WHEN month=4  THEN 'semester2'
                                WHEN month=5  THEN 'semester2'
                                WHEN month=6  THEN 'semester2'
                                WHEN month=7  THEN 'summer_vacation'
                                WHEN month=8  THEN 'summer_vacation'
                                WHEN month=9  THEN 'semester1'
                                WHEN month=10 THEN 'semester1'
                                WHEN month=11 THEN 'semester1'
                                WHEN month=12 THEN 'semester1'
                           END AS session_type,
                           CASE
                                WHEN month=10 THEN 'stable'
                                WHEN month=11 THEN 'stable'
                                WHEN month=12 THEN 'stable'
                                WHEN month=3  THEN 'stable'
                                WHEN month=4  THEN 'stable'
                                WHEN month=5  THEN 'stable'
                                WHEN month=7  THEN 'undefined'
                                WHEN month=8  THEN 'undefined'
                                WHEN month=2  THEN 'undefined'
                                ELSE 'unstable'
                           END AS semester_stable
                    FROM (",q.total,")",sep="")

  q.total <- paste("SELECT user_primary_key, points_earned, proficiency_status, video_complete_status, eventID, eventType, date, IP,
                           time_consumed, week_of_year, year, month, weekday, hour, ten_min_period, session_type, semester_stable
                    FROM (",q.total,")",sep="")

  q.total <- Ljoin(q.total, q.TANET.IP, "IP")    #left join

  #
  q.total <- paste("SELECT user_primary_key, points_earned, proficiency_status, video_complete_status, eventID, eventType, date, IP,
                           time_consumed, week_of_year, year, month, weekday, hour, ten_min_period,
                           IFNULL(isTANET,0) AS isTANET, session_type, semester_stable
                    FROM (",q.total,")",sep="")

  q.total <- paste("SELECT user_primary_key, points_earned, proficiency_status, video_complete_status, eventID, eventType, date, IP,
                           time_consumed, week_of_year, year, month, weekday, hour, ten_min_period, isTANET, session_type, semester_stable
                           FROM (",q.total,")",sep="")

  q.total <- paste("SELECT user_primary_key, eventID, eventType,
                           sum(points_earned) AS points_earned,
                           IFNULL(max(proficiency_status),0) AS proficiency_status,
                           IFNULL(max(video_complete_status),0) AS video_complete_status,
                           sum(time_consumed) AS time_consumed,
                           IP, isTANET,
                           date, week_of_year, year, month, weekday, hour, ten_min_period, session_type, semester_stable
                    FROM (",q.total,")
                    GROUP BY user_primary_key, eventID, eventType, IP, isTANET,
                             date, week_of_year, year, month, weekday, hour, ten_min_period, session_type, semester_stable",sep="")

  #check whether table exists, if not then create a new one (unless force is true)
  #as.POSIXct(as.numeric(get_table("junyiacademy","pcboy","tbltsclass")$creationTime)/1000,origin="1970-01-01") %>% as.Date

  return(write_bq_dataset(destination.dataset, tablename, force, q.total))

}#end function
