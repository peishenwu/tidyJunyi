#' A workable alternative for dplyr::semi_join to use in junyi bigquery datasets
#' @description return all rows from x where there are matching values in y, keeping just columns from x.
#' @param x tbl object
#' @param y tbl object
#' @param by join by column, can only take one value
#' @param by.x join by column in argument x
#' @param by.y join by column in argument y
#' @param tablename if not specified, then a random name will be generated
#' @return returns dplyr tbl object
#' @export

junyi.semi_join <- function(x, y, by = NULL, by.x = NULL, by.y = NULL, tablename = NULL){

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

  join.sql <- paste("SELECT * FROM (",x.sql,")
                     WHERE ",by.x," IN (",paste("SELECT ",by.y," FROM (",y.sql,")",sep=""),")",sep="")

  ##
  results <- query_exec(join.sql, "junyiacademy", max_pages = 1, page_size = 0, warn = F,
                        destination_table = paste(destination.dataset,".",tablename,sep=""),
                        write_disposition = "WRITE_TRUNCATE") #overwrite

  message(paste("temporary table created at ",destination.dataset,".",tablename,sep=""))
  return(tbl(DB, tablename))

}#end function
