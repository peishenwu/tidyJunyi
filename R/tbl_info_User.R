#' create tbl objects of various junyi bigquery datasets
#' @description  user's background data, the primary key of this table is user_primary_key
#' @details while most variables are unique for each user, fields userRole, userCity, userSchool exist multiple values, in which are separated by comma ","
#' @param force create a new data table despite a previous table with a same name already exist
#' @param load.only set to true to directly load pre-built tbl without building
#' @return returns dplyr tbl object
#' @export

tbl_info_User <- function(force = F, load.only = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]
  dataset.date <- tidyJunyi.settings[['backup.dataset']]

  #output table name
  tablename = "infousrdat"

  ##prepare
  dataset.date <- as.Date(dataset.date)
  dataset.date <- gsub("-|/","",dataset.date)

  ##about final table specs
  #a.user.email as userEmail,
  #a.user_email as userSendableEmail, a.user_id as userId,
  #a.user_role as userRole
  ##
  q.FT <- "SELECT userEmail AS user_primary_key,
                  userSendableEmail,
                  userGrade, userBirthdate
           FROM FinalTable.UserFinalTmpInfo"

  ##however, multiple tiers still exist in userRole, userCity, userSchool
  q.FT.userRole <- "SELECT userEmail AS user_primary_key, userRole AS userRole FROM [FinalTable.UserFinalTmpInfo]
                    WHERE userRole IS NOT NULL AND userRole <>'' GROUP BY user_primary_key, userRole"

  q.FT.userCity <- "SELECT userEmail AS user_primary_key, userCity AS userCity FROM [FinalTable.UserFinalTmpInfo]
                    WHERE userCity IS NOT NULL AND userCity <>'' GROUP BY user_primary_key, userCity"

  q.FT.userSchool <- "SELECT userEmail AS user_primary_key, userSchool AS userSchool FROM [FinalTable.UserFinalTmpInfo]
                      WHERE userSchool IS NOT NULL AND userSchool <>'' GROUP BY user_primary_key, userSchool"

  #aggregate mutiple values of the same user into a single value separated by comma
  q.FT.userRole <- paste("SELECT user_primary_key, GROUP_CONCAT(userRole) AS userRole FROM (",q.FT.userRole,") GROUP BY user_primary_key",sep="")
  q.FT.userCity <- paste("SELECT user_primary_key, GROUP_CONCAT(userCity) AS userCity FROM (",q.FT.userCity,") GROUP BY user_primary_key",sep="")
  q.FT.userSchool <- paste("SELECT user_primary_key, GROUP_CONCAT(userSchool) AS userSchool FROM (",q.FT.userSchool,") GROUP BY user_primary_key",sep="")

  ##complete q.FT
  q.FT <- Ljoin(q.FT, q.FT.userRole, "user_primary_key")
  q.FT <- Ljoin(q.FT, q.FT.userCity, "user_primary_key")
  q.FT <- Ljoin(q.FT, q.FT.userSchool, "user_primary_key")

  ##the primary key of user info table is user.email
  user.q <- paste("SELECT user.email AS user_primary_key,
                  __key__.name AS keyID,
                  user_id AS userID,
                  user_email AS underline_email,
                  current_user.email AS cu_email,
                  gender, points, user_nickname, username, joined AS joinedTime,
                  IF(INSTR(user_id,'nouserid') == 0,1,0) AS is_registered
                  FROM junyi_",dataset.date,".UserData_",dataset.date,sep="")

  user.q <- paste("SELECT user_primary_key, keyID, userID, underline_email, cu_email,
                          gender, points, user_nickname, username, is_registered, joinedTime
                   FROM (",user.q,")",sep="")

  result <- Ljoin(user.q, q.FT, "user_primary_key") #left join

  ##has_has_coach_count, has_has_trainee_count
  student_teacher_relation <- paste("SELECT coaches AS teacher_user_primary_key,
                                            user.email AS student_user_primary_key
                                     FROM [junyi_",dataset.date,".UserData_",dataset.date,"]",sep="")

  #those who coach themselves
  self_coach_users <- paste("SELECT student_user_primary_key AS user_primary_key, 1 AS self_coach
                             FROM (",student_teacher_relation,")
                             WHERE student_user_primary_key == teacher_user_primary_key",sep="")

  #calculate every user's student and teacher by counting primary key
  teachers_student_count <- paste("SELECT teacher_user_primary_key AS user_primary_key,
                                          EXACT_COUNT_DISTINCT(student_user_primary_key) AS count, 'student' AS type
                                   FROM (",student_teacher_relation,") GROUP BY user_primary_key",sep="")

  students_teacher_count <- paste("SELECT student_user_primary_key AS user_primary_key,
                                          EXACT_COUNT_DISTINCT(teacher_user_primary_key) AS count, 'teacher' AS type
                                   FROM (",student_teacher_relation,") GROUP BY user_primary_key",sep="")
  ##prepare for bqunion
  teachers_student_count <- paste("SELECT user_primary_key, count, type FROM (",teachers_student_count,")",sep="")
  students_teacher_count <- paste("SELECT user_primary_key, count, type FROM (",students_teacher_count,")",sep="")

  user_teacher_student_count <- bqunion(c(teachers_student_count,students_teacher_count))

  user_teacher_student_count <- paste("SELECT user_primary_key,
                                       IF(type == 'teacher',count,0) AS has_coach_count,
                                       IF(type == 'student',count,0) AS has_trainee_count
                                       FROM (",user_teacher_student_count,")",sep="")

  user_teacher_student_count <- paste("SELECT user_primary_key,
                                       SUM(has_coach_count) AS has_coach_count,
                                       SUM(has_trainee_count) AS has_trainee_count
                                       FROM (",user_teacher_student_count,") GROUP BY user_primary_key",sep="")

  result <- Ljoin(result, user_teacher_student_count, "user_primary_key")
  result <- Ljoin(result, self_coach_users, "user_primary_key")

  ##  has_class_count, belongs_to_class_count
  student.q <- paste("SELECT __key__.name AS student_keyID, user_email AS student_underline_email,
                            student_lists.path AS classID
                      FROM junyi_",dataset.date,".UserData_",dataset.date,sep="")

  teacher.q <- paste("SELECT coaches.name AS teacher_keyID,
                      __key__.path AS classID,
                      code AS classcode, name AS classname
                      FROM junyi_",dataset.date,".StudentList_",dataset.date,sep="")

  teacher_student_class_relationship.q <- Ijoin(paste("SELECT student_keyID, classID, student_underline_email FROM FLATTEN((",student.q,"), classID)",sep=""),
                                                paste("SELECT teacher_keyID, classID FROM(",teacher.q,")",sep=""),
                                                "classID")

  #calculate every user's class has and belongs to count
  user_belong_class.q <- paste("SELECT student_keyID AS keyID,
                                       EXACT_COUNT_DISTINCT(classID) AS count, 'belong' AS type
                                FROM (",teacher_student_class_relationship.q,") GROUP BY keyID",sep="")

  user_has_class.q <- paste("SELECT teacher_keyID AS keyID,
                                    EXACT_COUNT_DISTINCT(classID) AS count, 'has' AS type
                             FROM (",teacher_student_class_relationship.q,") GROUP BY keyID",sep="")

  user_belong_class.q <- paste("SELECT keyID, count, type FROM (",user_belong_class.q,")",sep="")
  user_has_class.q <- paste("SELECT keyID, count, type FROM (",user_has_class.q,")",sep="")

  user_class.q <- bqunion(c(user_belong_class.q,user_has_class.q))

  user_class.q <- paste("SELECT keyID,
                                IF(type == 'belong',count,0) AS belongs_to_class_count,
                                IF(type == 'has',count,0) AS has_class_count
                         FROM (",user_class.q,")",sep="")

  user_class.q <- paste("SELECT keyID,
                                SUM(belongs_to_class_count) AS belongs_to_class_count,
                                SUM(has_class_count) AS has_class_count
                         FROM (",user_class.q,") GROUP BY keyID",sep="")

  result <- Ljoin(result, user_class.q, "keyID")

  ##keep only user_primary_key --> for tidy dataset principle
  result <- paste("SELECT user_primary_key, gender, points, user_nickname, username, is_registered,
                          userSendableEmail, joinedTime, userBirthdate, userGrade,
                          userRole, userCity, userSchool,
                          IFNULL(has_coach_count,0) AS has_coach_count,
                          IFNULL(self_coach,0) AS self_coach,
                          IFNULL(has_trainee_count,0) AS has_trainee_count,
                          IFNULL(belongs_to_class_count,0) AS belongs_to_class_count,
                          IFNULL(has_class_count,0) AS has_class_count
                   FROM (",result,")",sep="")

  ##make it tidy by ensuring each row has only an unique primary key
  #result <- unique.query(result)

  #check whether table exists, if not then create a new one (unless force is true)
  return(write_bq_dataset(destination.dataset, tablename, force, result, bypass = load.only))

}#end function
