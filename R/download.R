#' download remote tbl into local data.frame object
#' @param x a tbl object
#' @param stringAsFactors convert columns of character-type into factors
#' @export
#'
download <- function(tbl, stringAsFactors = T){

  message("downloading remote tbl...")
  result <- tbl %>% collect %>% as.data.frame

  ##turn characters into factors for the compatibility of RAnalyticFlow
  if(stringAsFactors){
    char.col <- sapply(1:ncol(result),
                       function(x){ifelse(sum(grepl("character",class(result[,x])))!=0,x,NA)})
    char.col <- char.col[complete.cases(char.col)]

    for(col in char.col){
      result[,col] <- as.factor(result[,col])
    }#end for
  }#end if

  ##return
  return(result)

}#end function


