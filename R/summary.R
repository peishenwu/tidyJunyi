#' helper functions that make tbl objects like data.frames
#' @description  an alternative summary which works for tbl objects
#' @param x see ?summary for details, this can also be tbl objects
#' @param column the column containing values to be calculated
#' @export

summary <- function(x, column){
  if(sum("tbl_bigquery" == class(x))!=0){

    ##support for NSE
    parameters <- as.list(match.call())
    column <- as.character(parameters[['column']])

    ##
    sql <- extract_and_modify_sql(x)
    sql <- paste("SELECT MIN(",column,") AS Min, NTH(25,QUANTILES(",column,",101)) AS Q25,
                         NTH(50,QUANTILES(",column,",101)) AS Median, AVG(",column,") AS Mean,
                         NTH(75,QUANTILES(",column,",101)) AS Q75, MAX(",column,") AS Max,
                         NTH(33,QUANTILES(",column,",101)) AS Q33,
                         NTH(66,QUANTILES(",column,",101)) AS Q66
                  FROM (",sql,")",sep="")

    results <- query_exec(sql, "junyiacademy",
                          max_pages = 1,
                          page_size = 1000,
                          warn = F)

    names(results) <- c("Min.","1st Qu.","Median","Mean","3rd Qu.","Max.", "33%","66%")
    return(results)

  }else{
    base::summary(x)
  }#end if
}#end function

