#' create tbl objects of various junyi bigquery datasets
#' @description  teacher and student relationships, defined by coach-trainee relationship
#' @param force create a new data table despite a previous table with a same name already exist
#' @return returns dplyr tbl object
#' @export
#'

tbl_relation_users_TS_byCoach <- function(force = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name
  tablename = "reltscoach"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)
  #
  student_teacher_relation <- paste("SELECT coaches AS teacher_user_primary_key,
                                            user.email AS student_user_primary_key
                                     FROM [junyi_",dataset.date,".UserData_",dataset.date,"]",sep="")

  student_teacher_relation <- paste("SELECT teacher_user_primary_key, student_user_primary_key
                                     FROM FLATTEN((",student_teacher_relation,"),teacher_user_primary_key)",sep="")

  ##keep some node information, although duplicate, it is useful
  self_coach.q <- paste("SELECT teacher_user_primary_key, 1 AS self_coach
                         FROM (",student_teacher_relation,")
                         WHERE teacher_user_primary_key == student_user_primary_key",sep="")

  trainee_count.q <- paste("SELECT teacher_user_primary_key,
                                   EXACT_COUNT_DISTINCT(student_user_primary_key) AS teacher_has_trainee
                            FROM (",student_teacher_relation,")
                            GROUP BY teacher_user_primary_key",sep="")

  trainee_count.q <- Ljoin(trainee_count.q, self_coach.q, "teacher_user_primary_key")

  ##
  coach_count.q <- paste("SELECT student_user_primary_key,
                                 EXACT_COUNT_DISTINCT(teacher_user_primary_key) AS student_has_coach
                          FROM (",student_teacher_relation,")
                          GROUP BY student_user_primary_key",sep="")

  student_teacher_relation <- Ljoin(student_teacher_relation, trainee_count.q, "teacher_user_primary_key")
  student_teacher_relation <- Ljoin(student_teacher_relation, coach_count.q, "student_user_primary_key")

  #check whether table exists, if not then create a new one (unless force is true)
  return(write_bq_dataset(destination.dataset, tablename, force,
                          student_teacher_relation))

}#end function


