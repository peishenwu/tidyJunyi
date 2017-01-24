#' build all tidy tbls
#' @param force create a new data table despite a previous table with a same name already exist
#' @param load.only set to true to directly load pre-built tbls without building them
#' @param info include info tables
#' @param log include log tables
#' @param relation include relation tables
#' @return returns dplyr tbl object
#' @export

build_primary_tbls <- function(force = F, load.only = F, info = T, log = T, relation = T){

  #info
  if(!load.only){message("building info tables")}
  if(info){
    assign("info_user", tbl_info_User(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("info_content", tbl_info_Content(force = force, load.only = load.only), envir = .GlobalEnv)
  }#end if

  #log
  if(!load.only){message("building log tables")}
  if(log){
    assign("log_mission_assign", tbl_log_AssignMission(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("log_mission_do", tbl_log_DoingMission(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("log_videoprob", tbl_log_VideoProb(force = force, load.only = load.only), envir = .GlobalEnv)

    assign("log_probattempt", tbl_log_ProbAttempt(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("log_probquiz", tbl_log_ProbQuiz(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("log_videoplay", tbl_log_VideoPlay(force = force, load.only = load.only), envir = .GlobalEnv)
  }#end if

  #relation
  if(!load.only){message("building relationship tables")}
  if(relation){
    assign("rel_tscoach", tbl_relation_users_TS_byCoach(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("rel_tsclass", tbl_relation_users_TS_byClass(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("rel_sscoach", tbl_relation_users_SS_byCoach(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("rel_ssclass", tbl_relation_users_SS_byClass(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("rel_userip", tbl_relation_users_bySameIP(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("rel_userid", tbl_relation_user_identifiers(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("rel_probquiz", tbl_relation_prob_quiz(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("rel_mission_content", tbl_relation_mission_content(force = force, load.only = load.only), envir = .GlobalEnv)
    assign("rel_content_proximity", tbl_relationship_content_proximity(force = force, load.only = load.only), envir = .GlobalEnv)

  }#end if

  ##
  if(!load.only){message("the building process has completed...")}
}#end function





