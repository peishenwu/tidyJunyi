#' create tbl objects of various junyi bigquery datasets
#' @description  the shelf proximity of different contents that display at the Junyi's website
#' @param force create a new data table despite a previous table with a same name already exist
#' @param load.only set to true to directly load pre-built tbl without building
#' @return returns dplyr tbl object
#' @export

tbl_relationship_content_proximity <- function(force = F, load.only = F){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]

  #output table name
  tablename = "Relation_ContentProximity"

  ##
  max_time.q <- "SELECT MAX(make_default_time) AS make_default_time FROM [FinalTable.ContentFinalInfo]"
  content.q <- paste("SELECT content_name AS contentID,
                             content_pretty_name, content_kind, content_live, level1_name, level2_name, level3_name, level4_name, level5_name, make_default_time
                      FROM [FinalTable.ContentFinalInfo]
                      WHERE make_default_time IN (",max_time.q,")", sep="")

  content.q1 <- paste("SELECT contentID AS contentID1, content_pretty_name AS title1, content_kind AS type1,
                              level1_name, level2_name, level3_name, level4_name, level5_name,
                              CONCAT(level1_name, level2_name, level3_name, level4_name, level5_name) AS key
                       FROM (",content.q,")",sep="")

  content.q2 <- paste("SELECT contentID AS contentID2, content_pretty_name AS title2, content_kind AS type2,
                              CONCAT(level1_name, level2_name, level3_name, level4_name, level5_name) AS key
                      FROM (",content.q,")",sep="")

  ##prepare for join
  content.q1 <- paste("SELECT contentID1, title1, type1, key,
                              level1_name, level2_name, level3_name, level4_name, level5_name
                       FROM (",content.q1,")",sep="")
  content.q2 <- paste("SELECT contentID2, title2, type2, key FROM (",content.q2,")",sep="")
  result <- Ijoin(content.q1,content.q2,"key")
  result <- paste("SELECT contentID1, title1, type1, contentID2, title2, type2,
                          level1_name, level2_name, level3_name, level4_name, level5_name
                   FROM (",result,") WHERE contentID1 <> contentID2",sep="")

  #count items in this category
  category.count <- paste("SELECT contentID1, EXACT_COUNT_DISTINCT(contentID2) AS other_content_count
                           FROM (",result,") GROUP BY contentID1",sep="")
  category.count <- paste("SELECT contentID1, other_content_count FROM (",category.count,")",sep="")

  #merge
  result <- Ljoin(result, category.count, "contentID1")

  #check whether table exists, if not then create a new one (unless force is true)
  return(write_bq_dataset(destination.dataset, tablename, force, result, bypass = load.only))

}#end function

