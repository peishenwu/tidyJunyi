#' helper functions that make tbl objects like data.frames
#' @description  an alternative View which works for tbl objects
#' @param x see ?View for details, this can also be tbl objects
#' @param title see ?View for details
#' @param n how many rows to show, by default is 1000. Can be set to Inf if needed
#' @param func shows view or prints out head or returns colnames
#' @import bigrquery
#' @import stringr
#' @import dplyr
#' @export

view <- function(x, title, n = 5000, func = c("view","head","colnames")){

  ##use global variable
  destination.dataset <- tidyJunyi.settings[['destination.dataset']]

  func <- match.arg(func)
  if(func == "head"){n <- 10} #mimick utils::head()

  if(sum("tbl_bigquery" == class(x))!=0){
    res <- nrow(x)
    if(res>5000 & n == 5000 & func == "view"){message(paste("response has ",res," rows, only top 5000 rows are shown, use n = Inf to retrieve all",sep = ""))}

    sql <- extract_and_modify_sql(x)
    sql <- paste("SELECT * FROM (",sql,") ",ifelse(n == Inf,"",paste("LIMIT ",n,sep="")),sep="")

    results <- query_exec(sql, "junyiacademy",
                          max_pages = ifelse(n == Inf, Inf, 1),
                          page_size = ifelse(n == Inf, 10000, n),
                          warn = F)
    #show
    parameters <- as.list(match.call())

    if(func == "view"){

      x.title <- deparse(parameters[['x']])
      if(length(x.title)>1){x.title <- x.title[1]}
      if(nchar(x.title)>10){x.title <- paste(substr(x.title,1,10),"...",sep="")}

      View(results, title = x.title)
    }#end if

    if(func == "head"){
      print(results)
    }#end if

    if(func == "colnames"){
      return(colnames(results))
    }#end if

  }else{
    View(x, title)
  }#end if

}#end function
