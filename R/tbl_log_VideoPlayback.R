#' create tbl objects of various junyi bigquery datasets
#' @description  log data of video playback/reverse/forwarding
#' @param force create a new data table despite a previous table with a same name already exist
#' @param load.only set to true to directly load pre-built tbl without building
#' @return returns dplyr tbl object
#' @export

tbl_log_VideoPlayback <- function(force = F, load.only = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name (a-z, lower case, 10 characters)
  tablename = "logvidplay"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)
  ##
  q.video <- paste("SELECT user.email AS user_primary_key, time_watched AS timestamp, youtube_id AS eventID,
                           seconds_watched AS SW, last_second_watched AS LSW,
                           LAG(last_second_watched) OVER (PARTITION BY user.email, youtube_id
                                                          ORDER BY time_watched ASC) AS LLSW
                    FROM [junyi_",dataset.date,".VideoLog_",dataset.date,"]",sep="")

  q.video <- paste("SELECT user_primary_key, timestamp, eventID,
                           IF(SW+LLSW-LSW < -5,1,0) AS forward,
                           IF(SW+LLSW-LSW > 5,1,0) AS reverse,
                           IF(ABS(LLSW-LSW+SW)<5 OR LLSW IS NULL,1,0) AS playback,
                           IFNULL(LLSW,0) AS start_timeframe, LSW-SW AS end_timeframe
                    FROM (",q.video,")",sep="")

  q.video <- paste("SELECT user_primary_key, timestamp, eventID, forward, reverse, playback, start_timeframe, end_timeframe,
                           LEAD(start_timeframe) OVER (PARTITION BY user_primary_key, eventID
                                                       ORDER BY timestamp ASC) AS lead_STF
                    FROM (",q.video,")",sep="")

  q.video <- paste("SELECT user_primary_key, timestamp, eventID, forward, reverse, playback, start_timeframe, end_timeframe,
                           IF(playback == 1,IFNULL(lead_STF-start_timeframe,0),NULL) AS playback_length,
                           LAG(end_timeframe) OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp ASC) AS lag_ETF,
                           LAG(playback) OVER (PARTITION BY user_primary_key, eventID ORDER BY timestamp ASC) AS lag_playback
                    FROM (",q.video,")",sep="")

  q.video <- paste("SELECT user_primary_key, timestamp, eventID, forward, reverse, playback, playback_length,
                           CASE
                                WHEN playback == 1 AND lag_playback <> 1 THEN lag_ETF
                                WHEN playback == 1 AND lag_playback == 1 THEN end_timeframe
                                ELSE start_timeframe
                           END AS start_timeframe,
                           IF(playback == 1,end_timeframe + playback_length, end_timeframe) AS end_timeframe
                    FROM (",q.video,")",sep="")

  q.video <- paste("SELECT user_primary_key, timestamp, eventID, forward, reverse, playback, playback_length,
                           IF(start_timeframe == -1,0,start_timeframe) AS start_timeframe,
                           IF(end_timeframe == -1,0, end_timeframe) AS end_timeframe
                    FROM (",q.video,")",sep="")
  q.video <- paste("SELECT user_primary_key, timestamp, eventID, forward, reverse, playback,
                           start_timeframe, end_timeframe, playback_length FROM (",q.video,")",sep="")

  q.videoInfo <- paste("SELECT youtube_id AS eventID, duration AS total_timeframe
                        FROM [junyi_",dataset.date,".Video_",dataset.date,"]",sep="")
  #q.videoInfo <- paste("SELECT eventID, total_timeframe FROM (",q.videoInfo,")",sep="")
  #
  q.video <- Ljoin(q.video, q.videoInfo, "eventID")

  ##
  return(write_bq_dataset(destination.dataset, tablename, force, q.video, bypass = load.only))

}#end function
