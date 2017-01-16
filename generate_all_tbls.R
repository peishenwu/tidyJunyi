library(tidyJunyi) #must use version > 0.3

if(as.numeric(as.character(packageVersion("tidyJunyi"))) < 0.3){
  stop("please upgrade tidyJunyi package to version 0.3 or above")
}#end if

base_monday = as.Date("2016-1-4")
recent_monday <- base_monday + (as.numeric(Sys.Date() - base_monday) %/% 7)*7

set_dataset(backup.dataset.date = as.character(recent_monday),
            destination.dataset.name = "tmp_tidy")

#info
usertbl <- tbl_info_User(force = T)
contenttbl <- tbl_info_Content(force = T)

#log
do_mission <- tbl_log_AssignMission(force = T)
assign_mission <- tbl_log_DoingMission(force = T)
logtbl <- tbl_log_VideoProb(force = T)

#relation
userIDtbl <- tbl_relation_user_identifiers(force = T)
ts_class <- tbl_relation_users_TS_byClass(force = T)
ss_class <- tbl_relation_users_SS_byClass(force = T)
ts_coach <- tbl_relation_users_TS_byCoach(force = T)
ss_coach <- tbl_relation_users_SS_byCoach(force = T)
mission_content <- tbl_relation_content_byMission(force = T)






