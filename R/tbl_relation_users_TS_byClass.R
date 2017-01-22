#' create tbl objects of various junyi bigquery datasets
#' @description  teacher and student relationships, defined by classID
#' @param force create a new data table despite a previous table with a same name already exist
#' @param load.only set to true to directly load pre-built tbl without building
#' @return returns dplyr tbl object
#' @export

tbl_relation_users_TS_byClass <- function(force = F, load.only = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name
  tablename = "Relation_TeacherStudent_byClass"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)
  #
  student.q <- paste("SELECT __key__.name AS student_keyID,
                           student_lists.path AS classID
                     FROM junyi_",dataset.date,".UserData_",dataset.date,sep="")

  teacher.q <- paste("SELECT coaches.name AS teacher_keyID,
                     __key__.path AS classID,
                     code AS class_code, name AS class_name
                     FROM junyi_",dataset.date,".StudentList_",dataset.date,sep="")

  teacher_student_class_relationship.q <- Ijoin(paste("SELECT student_keyID, classID FROM FLATTEN((",student.q,"), classID)",sep=""),
                                                paste("SELECT teacher_keyID, classID, class_code, class_name FROM FLATTEN((",teacher.q,"), teacher_keyID)",sep=""),
                                                "classID")

  ##turn keyID into user_primary_key
  teacher_student_class_relationship.q <- Ijoin(teacher_student_class_relationship.q,
                                                user_connector_tbl(dataset.date, user_primary_key = "student_user_primary_key",
                                                                   keyID = "student_keyID", use.keyID = T), "student_keyID")

  teacher_student_class_relationship.q <- Ijoin(teacher_student_class_relationship.q,
                                                user_connector_tbl(dataset.date, user_primary_key = "teacher_user_primary_key",
                                                                   keyID = "teacher_keyID", use.keyID = T), "teacher_keyID")

  teacher_student_class_relationship.q <- paste("SELECT classID, teacher_user_primary_key, student_user_primary_key, class_code, class_name
                                                 FROM (",teacher_student_class_relationship.q,")",sep="")
  ##
  ##class size
  class_size.q <- paste("SELECT classID, EXACT_COUNT_DISTINCT(student_user_primary_key) AS class_size
                         FROM (",teacher_student_class_relationship.q,") GROUP BY classID",sep="")

  teacher_student_class_relationship.q <- Ljoin(teacher_student_class_relationship.q, class_size.q, "classID")
  ##
  teacher_student_class_relationship.q <- unique.query(teacher_student_class_relationship.q)

  #check whether table exists, if not then create a new one (unless force is true)

  return(write_bq_dataset(destination.dataset, tablename, force,
                          teacher_student_class_relationship.q, bypass = load.only))

}#end function
