#' A workable alternative for dplyr::right_join to use in junyi bigquery datasets
#' @param x tbl object
#' @param y tbl object
#' @param by join by column, can take a vector of names. eg. c("keyID","date)
#' @param by.x join by column in argument x
#' @param by.y join by column in argument y
#' @param restore.names restore input column names or keep the pre-fix of joined names
#' @param tablename if not specified, then a random name will be generated
#' @return returns dplyr tbl object
#' @export

junyi.right_join <- function(x, y, by = NULL, by.x = NULL, by.y = NULL, restore.names = T,
                            tablename = NULL){

  ##use global variable
  tidyJunyi.settings <- get("tidyJunyi.settings")
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]

  #initialize connection
  DB <- src_bigquery("junyiacademy", destination.dataset)
  #
  if(length(by)!=0){
    by.x <- by
    by.y <- by
  }else{
    if(length(by.x)==0 | length(by.y)==0){stop("either by or both by.x and by.y must be specified")}
  }#end if
  #
  x.sql <- extract_and_modify_sql(x)
  y.sql <- extract_and_modify_sql(y)

  if(length(tablename)==0){
    tablename <- get_rand_tablename(destination.dataset)
  }#end if

  #different versions if multiple join keys are provided
  if(length(by.x)==1 & length(by.y)==1){
    join.sql <- paste("SELECT * FROM (",x.sql,") AS pre RIGHT JOIN (",y.sql,") AS post ON pre.",by.x," = post.",by.y,sep="")
  }else{
    #get colnames
    keyname <- tolower(gsub("[[:space:]]","",paste(by.x, by.y, collapse = "")))
    x.sql <- paste("SELECT ",paste(colnames(x),collapse = ","),",
                   CONCAT(",paste(paste("string(",by.x,")",sep=""),collapse = ","),") AS ",keyname,"
                   FROM (",x.sql,")",collapse = "")

    y.sql <- paste("SELECT ",paste(colnames(y),collapse = ","),",
                   CONCAT(",paste(paste("string(",by.y,")",sep=""),collapse = ","),") AS ",keyname,"
                   FROM (",y.sql,")",collapse = "")

    join.sql <- paste("SELECT * FROM (",x.sql,") AS pre RIGHT JOIN (",y.sql,") AS post ON pre.",keyname," = post.",keyname,sep="")
  }#end if
  ##
  results <- query_exec(join.sql, "junyiacademy", max_pages = 1, page_size = 0, warn = F,
                        destination_table = paste(destination.dataset,".",tablename,sep=""),
                        write_disposition = "WRITE_TRUNCATE") #overwrite

  message(paste("temporary table created at ",destination.dataset,".",tablename,sep=""))

  result.tbl <- tbl(DB, tablename)
  work.colnames <- data.frame(original = colnames(result.tbl),
                              restore = gsub("^pre_|^post_","",colnames(result.tbl)))

  #remove the key generated from multiple keys
  if(!(length(by.x)==1 & length(by.y)==1)){
    work.colnames <- work.colnames[as.character(work.colnames$restore) != keyname,]
  }#end if

  #update work.colnames by keeping unique or columns from pre, and removing columns from post
  if(restore.names){work.colnames <- work.colnames[!duplicated(work.colnames$restore),]}
  result.tbl <- parse(text = paste("select(result.tbl,",
                                   paste(paste(work.colnames$restore,
                                               " = ",
                                               work.colnames$original,
                                               sep=""),
                                         collapse = ","),")",
                                   collapse="")) %>% eval
  return(result.tbl)
}#end function
