#' create tbl objects of various junyi bigquery datasets
#' @description  log data of video play/reverse/forwarding
#' @param force create a new data table despite a previous table with a same name already exist
#' @param load.only set to true to directly load pre-built tbl without building
#' @return returns dplyr tbl object
#' @export

tbl_log_VideoPlay <- function(force = F, load.only = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name (a-z, lower case, 10 characters)
  tablename = "Log_VideoPlay"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)
  ##

  q.video <- paste("SELECT user_primary_key, timestamp, eventID, forward, reverse, play,
                           IF(play=1 AND seq_index>1,start_frame+1,start_frame) AS start_frame,
                           IF(play=1 AND seq_index<>seq_total,end_frame-1,end_frame) AS end_frame,
                           IF(lead_SF IS NOT NULL AND ABS(end_frame-lead_SF)>1,end_frame+1,NULL) AS unrecord_play_SF,
                           IF(lead_SF IS NOT NULL AND ABS(end_frame-lead_SF)>1,
                              IF(seq_index<>seq_total,lead_SF-1,lead_SF),NULL) AS unrecord_play_EF,
                           FALSE AS is_recovered, seq_index, seq_total
                    FROM
                   (SELECT user_primary_key, timestamp, eventID, forward, reverse, play, start_frame, end_frame,
                    LEAD(start_frame) OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp) AS lead_SF,
                    ROW_NUMBER() OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp) AS seq_index,
                    COUNT(eventID) OVER (PARTITION BY user_primary_key, eventID) AS seq_total
                    FROM
                   (SELECT user_primary_key, timestamp, eventID, forward, reverse, play,
                           start_frame, end_frame
                    FROM
                   (SELECT user_primary_key, timestamp, eventID, forward, reverse, play, play_begin, play_end, start_frame,
                    IF(play_begin = 1 AND play_end = 0,updated_lead_EF, end_frame) AS end_frame
                    FROM
                   (SELECT user_primary_key, timestamp, eventID, forward, reverse, play, play_begin, play_end, start_frame, end_frame,
                    LEAD(end_frame) OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp) AS updated_lead_EF
                    FROM
                   (SELECT user_primary_key, timestamp, eventID, forward, reverse, play, play_begin, play_end,
                    IF(play_begin = 1,IFNULL(lag_EF,0),start_frame) AS start_frame,
                    IF(play_end = 1,IFNULL(lead_SF,end_frame),end_frame) AS end_frame
                    FROM
                   (SELECT user_primary_key, timestamp, eventID, forward, reverse, play, start_frame, end_frame,
                    LAG(start_frame) OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp) AS lag_SF,
                    LEAD(start_frame) OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp) AS lead_SF,
                    LAG(end_frame) OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp) AS lag_EF,
                    LEAD(end_frame) OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp) AS lead_EF,
                    IF(play = 1 AND play_begin = 0 AND play_end = 0,1,play_begin) AS play_begin,
                    IF(play = 1 AND play_begin = 0 AND play_end = 0,1,play_end) AS play_end
                    FROM
                   (SELECT user_primary_key, timestamp, eventID, forward, reverse, play, start_frame, end_frame, lag_play, lead_play,
                    IF(play = 1 AND IFNULL(lag_play,0) = 0 AND lead_play = 1,1,0) AS play_begin,
                    IF(play = 1 AND lag_play = 1 AND IFNULL(lead_play,0) = 0,1,0) AS play_end
                    FROM
                   (SELECT user_primary_key, timestamp, eventID, forward, reverse, play,
                    start_frame, end_frame, lag_play, lead_play,
                    FROM
                   (SELECT user_primary_key, timestamp, eventID, forward, reverse, play,
                    start_frame, end_frame, lag_play, lead_play,
                    IF(play = 1 AND lag_play = 1 AND lead_play = 1,0,1) AS include_row
                    FROM
                   (SELECT user_primary_key, timestamp, eventID, forward, reverse, play,
                    LAG(play) OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp) AS lag_play,
                    LEAD(play) OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp) AS lead_play,
                    start_frame, end_frame
                    FROM
                   (SELECT user_primary_key, timestamp, eventID,
                    IF(SW+LLSW-LSW < -1,1,0) AS forward,
                    IF(SW+LLSW-LSW > 1,1,0) AS reverse,
                    IF(ABS(LLSW-LSW+SW)<2 OR LLSW IS NULL,1,0) AS play,
                    IFNULL(LLSW,0) AS start_frame, LSW-SW AS end_frame
                    FROM
                   (SELECT user.email AS user_primary_key, time_watched AS timestamp, youtube_id AS eventID, seconds_watched AS SW, last_second_watched AS LSW,
                    LAG(last_second_watched) OVER (PARTITION BY user.email, youtube_id ORDER BY time_watched ASC) AS LLSW
                    FROM [junyi_",dataset.date,".VideoLog_",dataset.date,"]
                   ))))
                   WHERE include_row = 1)))))
                   WHERE play_end<>1)))",sep="")

  ##recover unrecorded plays
  #add one second to recovered timestamps to make the order slightly behind the original data

  q.video_recover <- paste("SELECT user_primary_key, DATE_ADD(timestamp, 1, 'SECOND') AS timestamp,
                                   eventID, 0 AS forward, 0 AS reverse,1 AS play,
                                   unrecord_play_SF AS start_frame, unrecord_play_EF AS end_frame,
                                   TRUE AS is_recovered, seq_index, seq_total
                            FROM (",q.video,")
                            WHERE unrecord_play_SF IS NOT NULL AND unrecord_play_EF IS NOT NULL",sep="")

  ##union of original data with recovered results
  q.video <- paste("SELECT user_primary_key, timestamp, eventID, forward, reverse, play, is_recovered,
                           start_frame, end_frame,
                           ROW_NUMBER() OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp) AS seq_index,
                           COUNT(eventID) OVER (PARTITION BY user_primary_key, eventID) AS seq_total
                    FROM (",q.video,"),(",q.video_recover,")
                    GROUP BY user_primary_key, timestamp, eventID, forward, reverse, play,
                             start_frame, end_frame, is_recovered, seq_index, seq_total",sep="")

  ##prepare for join
  q.video <- paste("SELECT user_primary_key, timestamp, eventID, forward, reverse, play, is_recovered,
                           start_frame, end_frame, seq_index, seq_total FROM (",q.video,")",sep="")
  q.videoInfo <- paste("SELECT youtube_id AS eventID, duration AS total_frame
                        FROM [junyi_",dataset.date,".Video_",dataset.date,"]",sep="")
  q.video <- Ljoin(q.video, q.videoInfo, "eventID")

  ##
  return(write_bq_dataset(destination.dataset, tablename, force, q.video, bypass = load.only))

}#end function
