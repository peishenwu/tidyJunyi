#' create tbl objects of various junyi bigquery datasets
#' @description  student and student relationships, defined by coach-trainee relationship (having a common teacher)
#' @param force create a new data table despite a previous table with a same name already exist
#' @return returns dplyr tbl object
#' @import bigrquery
#' @import stringr
#' @import dplyr
#' @export
#'

tbl_relation_users_SS_byCoach <- function(force = F){

  ##use global variable
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name
  tablename = "relsscoach"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)
  #
  student_teacher_relation <- paste("SELECT coaches AS teacher_user_primary_key,
                                            user.email AS student_user_primary_key
                                     FROM [junyi_",dataset.date,".UserData_",dataset.date,"]",sep="")

  student1_teacher_relation <- paste("SELECT student_user_primary_key AS student1_user_primary_key, teacher_user_primary_key
                                      FROM FLATTEN((",student_teacher_relation,"),teacher_user_primary_key)", sep = "")
  student2_teacher_relation <- paste("SELECT student_user_primary_key AS student2_user_primary_key, teacher_user_primary_key
                                      FROM FLATTEN((",student_teacher_relation,"),teacher_user_primary_key)", sep = "")

  student_student_relation <- Ijoin(student1_teacher_relation, student2_teacher_relation, "teacher_user_primary_key")
  student_student_relation <- paste("SELECT * FROM (",student_student_relation,")
                                     WHERE student1_user_primary_key <> student2_user_primary_key",sep="")

  #check whether table exists, if not then create a new one (unless force is true)
  return(write_bq_dataset(destination.dataset, tablename, force,
                          student_student_relation))

}#end function


