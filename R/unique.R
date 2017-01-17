#' helper functions that make tbl objects like data.frames
#' @description  an alternative unique which works for tbl objects
#' @param x tbl object
#' @export

unique <- function(x){

  if(sum("tbl_bigquery" == class(x))!=0){

    ##use global variable
    tidyJunyi.settings <- get("tidyJunyi.settings")
    destination.dataset <- tidyJunyi.settings[['destination.dataset']]

    x.columns <- view(x, func = "colnames") #get col name

    x.sql <- extract_and_modify_sql(x)
    x.sql <- paste("SELECT * FROM (",x.sql,") GROUP BY ",paste(x.columns,collapse=","),sep="")

    return(build_tbl_from_sql(x.sql))
  }else{
    return(base::unique(x))
  }#end if

}#end function

