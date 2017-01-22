#' create tbl objects of various junyi bigquery datasets
#' @description  student and student relationships, defined by classID
#' @param force create a new data table despite a previous table with a same name already exist
#' @param load.only set to true to directly load pre-built tbl without building
#' @return returns dplyr tbl object
#' @export

tbl_relation_users_SS_byClass <- function(force = F, load.only = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name
  tablename = "Relation_StudentStudent_byClass"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)
  #
  student.q <- paste("SELECT __key__.name AS student_keyID,
                      student_lists.path AS classID
                      FROM junyi_",dataset.date,".UserData_",dataset.date,sep="")

  classinfo.q <- paste("SELECT __key__.path AS classID, code AS class_code, name AS class_name
                        FROM junyi_",dataset.date,".StudentList_",dataset.date,sep="")

  student.q1 <- paste("SELECT student_keyID AS student1_keyID, classID FROM FLATTEN((",student.q,"), classID)",sep="")
  student.q2 <- paste("SELECT student_keyID AS student2_keyID, classID FROM FLATTEN((",student.q,"), classID)",sep="")

  student_student_class_relationship.q <- Ijoin(student.q1, student.q2, "classID")
  student_student_class_relationship.q <- Ijoin(student_student_class_relationship.q, classinfo.q, "classID")

  student_student_class_relationship.q <- paste("SELECT student1_keyID, classID, student2_keyID, class_code, class_name
                                                 FROM (",student_student_class_relationship.q,")
                                                 WHERE student1_keyID <> student2_keyID",sep = "")

  ##turn keyID into user_primary_key
  student_student_class_relationship.q <- Ijoin(student_student_class_relationship.q,
                                                user_connector_tbl(dataset.date, user_primary_key = "student1_user_primary_key",
                                                                   keyID = "student1_keyID", use.keyID = T), "student1_keyID")

  student_student_class_relationship.q <- Ijoin(student_student_class_relationship.q,
                                                user_connector_tbl(dataset.date, user_primary_key = "student2_user_primary_key",
                                                                   keyID = "student2_keyID", use.keyID = T), "student2_keyID")

  student_student_class_relationship.q <- paste("SELECT classID, student1_user_primary_key, student2_user_primary_key, class_code, class_name
                                                 FROM (",student_student_class_relationship.q,")",sep="")

  ##class size
  class_size.q <- paste("SELECT classID, EXACT_COUNT_DISTINCT(student1_user_primary_key) AS class_size
                        FROM (",student_student_class_relationship.q,") GROUP BY classID",sep="")

  student_student_class_relationship.q <- Ljoin(student_student_class_relationship.q, class_size.q, "classID")

  ##
  student_student_class_relationship.q <- unique.query(student_student_class_relationship.q)

  #check whether table exists, if not then create a new one (unless force is true)
  return(write_bq_dataset(destination.dataset, tablename, force,
                          student_student_class_relationship.q, bypass = load.only))

}#end function
