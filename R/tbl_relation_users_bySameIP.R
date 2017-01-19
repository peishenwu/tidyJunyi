#' create tbl objects of various junyi bigquery datasets
#' @description  users who are from the same IPs used by others during the same day at the same hour during the past 365 days
#' @param force create a new data table despite a previous table with a same name already exist
#' @param window.begin the date to begin watching, in format yy/mm/dd. If not specified, the beginning date will be the backup.dataset date - 365 days
#' @param window.end the date to end watching, in format yy/mm/dd. If not specified, the end date will be the backup.dataset date
#' @param load.only set to true to directly load pre-built tbl without building
#' @return returns dplyr tbl object
#' @export
#'

tbl_relation_users_bySameIP <- function(force = F, window.begin = NULL, window.end = NULL, load.only = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name
  tablename = "relusersip"

  ##prepare
  dataset.date <- as.Date(dataset.date)

  ##
  window.q.condition <- "ip_address IS NOT NULL"
  if(length(window.begin)!=0){
    window.begin <- as.Date(window.begin)
    window.begin <- gsub("/","-",window.begin)
    window.q.condition <- c(window.q.condition, paste("_timestamp >= TIMESTAMP('",window.begin,"')",sep=""))
  }else{
    window.q.condition <- c(window.q.condition, paste("_timestamp >= TIMESTAMP('",as.character(dataset.date-365),"')",sep=""))
  }#end if

  if(length(window.end)!=0){
    window.end <- as.Date(window.end)
    window.end <- gsub("/","-",window.end)
    window.q.condition <- c(window.q.condition, paste("_timestamp <= TIMESTAMP('",dataset.date,"')",sep=""))
  }else{

  }#end if

  dataset.date <- gsub("-|/","",dataset.date)
  ##
  q.IP.video <- paste("SELECT user.email AS dot_email, ip_address AS IP,
                              DATE(DATE_ADD(time_watched, 8, 'HOUR')) AS date,
                              HOUR(DATE_ADD(time_watched, 8, 'HOUR')) AS hour
                       FROM junyi_",dataset.date,".VideoLog_",dataset.date,"
                       WHERE ",gsub("_timestamp","time_watched",paste(window.q.condition,collapse=" AND ")),sep="")

  q.IP.prob <- paste("SELECT user.email AS dot_email, ip_address AS IP,
                             DATE(DATE_ADD(time_done, 8, 'HOUR')) AS date,
                             HOUR(DATE_ADD(time_done, 8, 'HOUR')) AS hour
                      FROM junyi_",dataset.date,".ProblemLog_",dataset.date,"
                      WHERE ",gsub("_timestamp","time_done",paste(window.q.condition,collapse=" AND ")),sep="")

  q.IP.video <- paste("SELECT dot_email, IP, date, hour,
                              CONCAT(STRING(IP),STRING(date),STRING(hour)) AS key
                       FROM (",q.IP.video,")",sep="")
  q.IP.prob <- paste("SELECT dot_email, IP, date, hour,
                             CONCAT(STRING(IP),STRING(date),STRING(hour)) AS key
                      FROM (",q.IP.prob,")",sep="")

  ##prepare union
  q.IP.video <- paste("SELECT dot_email, IP, date, hour, key FROM (",q.IP.video,")",sep="")
  q.IP.prob <- paste("SELECT dot_email, IP, date, hour, key FROM (",q.IP.prob,")",sep="")

  q.IP.user1 <- paste("SELECT dot_email AS dot_email1, IP, date, hour, key FROM (",bqunion(c(q.IP.video, q.IP.prob), unique = T),")",sep="")
  q.IP.user2 <- paste("SELECT dot_email AS dot_email2, key FROM (",bqunion(c(q.IP.video, q.IP.prob), unique = T),")",sep="")

  result <- Ijoin(q.IP.user1, q.IP.user2, "key")

  result <- paste("SELECT dot_email1 AS user1_primary_key, dot_email2 AS user2_primary_key, date, hour, IP, key
                   FROM (",result,") WHERE dot_email1 <> dot_email2",sep="")
  ##
  user.count <- paste("SELECT user1_primary_key, EXACT_COUNT_DISTINCT(user2_primary_key) AS other_user_count, date, hour, IP
                       FROM (",result,") GROUP BY date, hour, IP, user1_primary_key",sep="")

  user.count <- paste("SELECT CONCAT(STRING(IP),STRING(date),STRING(hour)) AS key, other_user_count
                       FROM (",user.count,")",sep="")

  ##prepare for join
  result <- paste("SELECT user1_primary_key, user2_primary_key, date, hour, IP, key FROM (",result,")",sep="")
  user.count <- paste("SELECT key, other_user_count FROM (",user.count,")",sep="")
  ##
  result <- Ljoin(result, user.count, "key")
  result <- paste("SELECT user1_primary_key, user2_primary_key,
                          date, YEAR(TIMESTAMP(date)) AS year, MONTH(TIMESTAMP(date)) AS month,
                          WEEK(TIMESTAMP(date)) AS week_of_year, DAYOFWEEK(TIMESTAMP(date)) AS weekday,
                          hour, other_user_count, IP FROM (",result,")",sep="")

  result <- paste("SELECT user1_primary_key, user2_primary_key, IP, date, year, month, week_of_year,
                          CASE
                                WHEN weekday=1 THEN 7
                                WHEN weekday=2 THEN 1
                                WHEN weekday=3 THEN 2
                                WHEN weekday=4 THEN 3
                                WHEN weekday=5 THEN 4
                                WHEN weekday=6 THEN 5
                                WHEN weekday=7 THEN 6
                          END AS weekday,
                          hour, other_user_count FROM (",result,")",sep="")
  ##
  return(write_bq_dataset(destination.dataset, tablename, force, result, bypass = load.only))

}#end function
