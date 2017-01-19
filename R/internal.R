#' @import bigrquery
#' @import dplyr
#' @importFrom stringr 'str_split'
#' @importFrom stringr 'str_extract_all'

## below are utility functions
Ijoin <- function(query1, query2, ON, restore.name = T){
  result <- paste("SELECT * FROM (",query1,") AS pre INNER JOIN (",
                  query2,") AS post ON pre.",ON," = post.",ON,
                  sep="")

  if(restore.name){
    ##prepare for auto-rename column names after join
    ##requirement: SELECT/AS/FROM must be in upper case
    pre.col <- getQueryColumns(query1)
    post.col <- getQueryColumns(query2)

    combine.col <- c(pre.col, post.col)
    duplicate.col <- combine.col[duplicated(combine.col)]
    post.col <- post.col[!(post.col %in% duplicate.col)]

    if(length(post.col)!=0){
      rename.col <- paste(paste(paste("pre.",pre.col," AS ",pre.col,sep=""),collapse=","),
                          paste(paste("post.",post.col," AS ",post.col,sep=""),collapse = ","),
                          sep = ",")
    }else{ rename.col <- paste(paste("pre.",pre.col," AS ",pre.col,sep=""),collapse=",") }
    ##
    result <- paste("SELECT ",rename.col," FROM (",result,")",sep="")
  }#end if restore name is needed
  result
}#end function

Ljoin <- function(query1, query2, ON, restore.name = T){
  result <- paste("SELECT * FROM (",query1,") AS pre LEFT JOIN (",
                  query2,") AS post ON pre.",ON," = post.",ON,
                  sep="")

  if(restore.name){
    ##prepare for auto-rename column names after join
    ##requirement: SELECT/AS/FROM must be in upper case
    pre.col <- getQueryColumns(query1)
    post.col <- getQueryColumns(query2)

    combine.col <- c(pre.col, post.col)
    duplicate.col <- combine.col[duplicated(combine.col)]
    post.col <- post.col[!(post.col %in% duplicate.col)]

    if(length(post.col)!=0){
      rename.col <- paste(paste(paste("pre.",pre.col," AS ",pre.col,sep=""),collapse=","),
                          paste(paste("post.",post.col," AS ",post.col,sep=""),collapse = ","),
                          sep = ",")
    }else{ rename.col <- paste(paste("pre.",pre.col," AS ",pre.col,sep=""),collapse=",") }
    ##
    result <- paste("SELECT ",rename.col," FROM (",result,")",sep="")
  }#end if restore name is needed
  result
}#end function


Ojoin <- function(query1, query2, ON, restore.name = T){
  result <- paste("SELECT * FROM (",query1,") AS pre OUTER JOIN (",
                  query2,") AS post ON pre.",ON," = post.",ON,
                  sep="")

  if(restore.name){
    ##prepare for auto-rename column names after join
    ##requirement: SELECT/AS/FROM must be in upper case
    pre.col <- getQueryColumns(query1)
    post.col <- getQueryColumns(query2)

    combine.col <- c(pre.col, post.col)
    duplicate.col <- combine.col[duplicated(combine.col)]
    post.col <- post.col[!(post.col %in% duplicate.col)]

    if(length(post.col)!=0){
      rename.col <- paste(paste(paste("pre.",pre.col," AS ",pre.col,sep=""),collapse=","),
                          paste(paste("post.",post.col," AS ",post.col,sep=""),collapse = ","),
                          sep = ",")
    }else{ rename.col <- paste(paste("pre.",pre.col," AS ",pre.col,sep=""),collapse=",") }
    ##
    result <- paste("SELECT ",rename.col," FROM (",result,")",sep="")
  }#end if restore name is needed
  result
}#end function

##tool function for joins: to extract the col names from SQL QUERY "SELECT {column names} FROM ..."
getQueryColumns <- function(querytext){
  results <- gsub("[[:space:]]","",
                  gsub(".*AS","",
                       str_split(gsub("SELECT","",
                                      str_split(gsub("\n","",querytext),"FROM")[[1]][1]),
                                 ",")[[1]]))
  results[nchar(results)!=0] ##remove "" empty ones
}#end function

##UNION FUNCTIONS
bqunion <- function(query_vectors, unique = F){
  result <- paste("SELECT ",paste(getQueryColumns(query_vectors[1]),collapse = ","),"
                   FROM ",paste(paste("(",query_vectors,")",sep=""),collapse = ","),sep="")
  if(unique){
    result <- paste("SELECT ",paste(getQueryColumns(query_vectors[1]),collapse = ","),"
                     FROM ",paste(paste("(",query_vectors,")",sep=""),collapse = ","),
                    " GROUP EACH BY ",paste(getQueryColumns(query_vectors[1]),collapse = ","),sep="")
  }#end if
  return(result)
}#end bqunion

##
unique.query <- function(query){
  return(paste("SELECT ",paste(getQueryColumns(query),collapse = ","),"
                FROM (",query,")
                GROUP BY ",paste(getQueryColumns(query),collapse = ","),sep=""))
}#end function

##
##utility functions to use for junyi's dplyr alternatives

extract_and_modify_sql <- function(tbl){

  ##extract dataset name from S4 object
  destination.dataset <- tbl$src$con@dataset
  ##

  sql <- gsub("\n"," ",(as.character(sql_render(tbl))))
  tablenames <- str_extract_all(sql,"FROM \\[[a-z][a-z][a-z][a-z][a-z][a-z][a-z][a-z][a-z][a-z]\\]")[[1]]
  tablenames <- gsub("\\[|\\]|FROM|[[:space:]]","",tablenames)
  replacenames <- paste(destination.dataset,".",tablenames,sep="")

  for(i in 1:length(tablenames)){
    sql <- gsub(tablenames[i],replacenames[i],sql)
  }#end for

  return(sql)
}#end function

get_rand_tablename <- function(destination.dataset){

  while(T){
    tablename = paste(sample(letters,10),collapse="") #generate a random temporary table name
    if(!is_table_exist(destination.dataset, tablename)){ break }#check whether it is a valid one or not
  }#end while

  return(tablename)
}#end function

write_bq_dataset <- function(destination.dataset, tablename, force, query, bypass=F){

  if(force | !is_table_exist(destination.dataset, tablename, bypass)){
    results <- query_exec(query, "junyiacademy", max_pages = 1, page_size = 0, warn = F,
                          destination_table = paste(destination.dataset,".",tablename,sep=""),
                          write_disposition = "WRITE_TRUNCATE") #overwrite
  }else{
    if(!bypass){
      message(paste("table ",destination.dataset,".",tablename," seems to exist already, loading it now...",sep=""))
    }else{
      message(paste("loading ",destination.dataset,".",tablename,sep=""))
    }#end if

  }#end if

  DB <- src_bigquery("junyiacademy", destination.dataset)
  return(tbl(DB, tablename))

}#end function

##due to only 50 tables will be shown by bq API functions, we manually record temporary tables created
is_table_exist <- function(destination.dataset, tablename, bypass=F){


  if(!bypass){
    job <- insert_upload_job(project = "junyiacademy",
                             dataset = destination.dataset,
                             table = "tidyJunyi_tbl_list",
                             values = data.frame(tablename = tablename,
                                                 timestamp = Sys.time(),
                                                 tidyjunyi_version = as.character(packageVersion("tidyJunyi")),
                                                 client_info = paste(Sys.info(),collapse="___")),
                             create_disposition = "CREATE_IF_NEEDED",
                             write_disposition = "WRITE_APPEND")

    wait_for(job, quiet = T)

    #check if count(tablename) > 1 (not including itself) then the name is used before (thus table exist)
    result <- query_exec(paste("SELECT COUNT(tablename) AS count
                               FROM ",destination.dataset,".tidyJunyi_tbl_list
                               WHERE tablename == '",tablename,"'",sep=""),
                         "junyiacademy", max_pages = 1, warn = F)

    if(result$count > 1){return(T)}else{return(F)}

  }else{
    return(T) ## set bypass is TRUE if already knew tbl exist... to speed up loading
  }#end if

}#end function

##this function is used to generate queries to turn user keys into user_primary_key
user_connector_tbl <- function(dataset.date, user_primary_key = "user_primary_key",
                               keyID = "keyID", userID = "userID",
                               underline_email = "underline_email", cu_email = "cu_email",
                               ##
                               use.keyID = F, use.uEmail = F,
                               use.cuEmail = F, use.userID = F){

  if(!grepl("\\d\\d\\d\\d\\d\\d\\d\\d",dataset.date)){
    dataset.date <- as.Date(dataset.date)
    dataset.date <- gsub("-|/","",dataset.date)
  }#end if

  user.q <- paste("SELECT user.email AS ",user_primary_key,",__key__.name AS ",keyID,",
                  user_id AS ",userID,",user_email AS ",underline_email,",current_user.email AS ",cu_email,"
                  FROM junyi_",dataset.date,".UserData_",dataset.date,sep="")

  user.q <- paste("SELECT ",user_primary_key,",",paste(c(keyID, userID, underline_email, cu_email)[c(use.keyID,use.userID, use.uEmail, use.cuEmail)],
                                                       collapse=",")," FROM (",user.q,")",sep="")

  return(user.q)
}#end function
