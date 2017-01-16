dataset.date = "20170109"

library(tidyJunyi)
set_dataset(backup.dataset.date = "2017-1-9",
            destination.dataset.name = "tmp_tidy")


student_teacher_relation <- paste("SELECT coaches AS teacher_id,
                                            user.email AS student_user_primary_key
                                  FROM [junyi_",dataset.date,".UserData_",dataset.date,"]
                                  WHERE coaches IS NOT NULL",sep="")

ts_rel <- build_tbl_from_sql(student_teacher_relation)
##
usertbl <- tbl_info_User(force = F)
userIDtbl <- tbl_relation_user_identifiers(force = F)
ts_class <- tbl_relation_users_TS_byClass(force = F)

##
those.teachers <- usertbl %>% filter(belongs_to_class_count > 0)  %>% select(user_primary_key) %>%
                              junyi.left_join(ts_class,
                                              by.x = "user_primary_key", by.y = "student_user_primary_key")  %>%
                  select(student_user_primary_key, teacher_user_primary_key) %>%
                  junyi.left_join(userIDtbl, by.x = "teacher_user_primary_key",
                                  by.y = "user_primary_key")

total_tbl <- junyi.inner_join(those.teachers, ts_rel, "student_user_primary_key") %>%
             mutate(is_keyID = IF(keyID == teacher_id,1,0),
                    is_userID = IF(userID == teacher_id,1,0),
                    is_uEmail = IF(underline_email == teacher_id,1,0),
                    is_cuEmail = IF(cu_email == teacher_id,1,0),
                    is_PK = IF(user_primary_key == teacher_id,1,0))

total_tbl  %>% summarise(keyID = sum(is_keyID),
                         userID = sum(is_userID),
                         uEmail = sum(is_uEmail),
                         cuEmail = sum(is_cuEmail),
                         PK  = sum(is_PK))


