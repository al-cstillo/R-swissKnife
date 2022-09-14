library(DBI)
library (dplyr)
library(tidyr)
library(odbc)
library(readr)
library(data.table)
library(writexl)
library(xlsx)
library(qdap)

-----------------------------------------------------------------------
getSQL = function(filepath){
  con = file(filepath, "r")
  sql.string <- ""
  while (TRUE){
    line <- readLines(con, n = 1)
    if ( length(line) == 0 ){
      break
    }
    line <- gsub("\\t", " ", line)
    if(grepl("--",line) == TRUE){
      line <- paste(sub("--","/*",line),"*/")
    }
    sql.string <- paste(sql.string, line)
  }
  close(con)
  return(paste0("set nocount on\n",sql.string))
}

----------------------------------------------------------------
  
#setwd("D:/Main/queries/ale/Auto hechos")
#path="CART_20xCOB.sql" 
#
#blok_7 ="
#set nocount on
#Use SISBR;
#Exec sisbr.dbo.USP_BI_FactReservaRiesgo  @EndOfDay ='YYYY-MM-DD';
#"
#
#blok_8 ="
#set nocount on
#Use SISBR;
#Exec dbo.USP_BI_FactAntiguedadSaldo @topdate='YYYY-MM-DD';
#"
#
#blok_11="
#set nocount on
#Use SISBR;
#Exec dbo.USP_BI_FACT_SINIESTROPENDIENTE @EndOfDay ='YYYY-MM-DD'
#"
#
#blok_12="
#set nocount on
#Use SISBR;
#exec dbo.USP_BI_FACT_CARTERA  @EndOfDay='YYYY-MM-DD'
#"
#
#
#query="
#set nocount on
#Use SISBR;
#exec [SP_FireStatistic] @dWDate='2020-09-30 23:59:59.999' ,@WithClaim=1,@cSlob = '(201,202,203,206)',@dEffectiveDate ='2020-09-30' ,@nCov=321
#"
#
#base = dbFetch(dbSendQuery(con,query))
#pattern = c("YYYY-MM-DD")
#
#
#
#query=getSQL(path)



#EOMs= seq(as.Date("2019-09-30"),as.Date("2001-01-01"),by="-1 days")
#EOMs= (seq(as.Date("2020-12-01"),as.Date("2019-01-01"),by="-1 months")-1) 
#EOMs= seq(as.Date("2014-10-31"),as.Date("2019-11-30"),by="1 years")

#dataa=dbFetch(dbSendQuery(con,query))

#EOMs=c(as.Date("2020-12-05") ,EOMs) 
#EOMs= c(seq(as.Date("2020-12-01"),as.Date("2017-02-01"),by="-1 months")-1) 

fun =function(repl,queryRoot,patt,trye,maxTry,sleep.time) {
  if(missing(trye))      {trye      =1 } 
  if(missing(maxTry))    {maxTry    =40} 
  if(missing(sleep.time)){sleep.time=30} 
  tmp<<-1
  #Does the try
  tryCatch(
    {
      #Crea una conexion cada vez que se ejecuta una fecha en la funcion
      con = dbConnect(odbc(), Driver = "SQL Server", 
                      Server = "172.22.43.20",
                      UID = "SISUSR", 
                      PWD = "123456@#",
                      encoding="Latin1")
      #imprime y guarda en el log que la conexion fue creada y a la hora x
      Conn_Flag = print(paste0("Connection created for day ",paste(repl,collapse = " : ")," at time ",Sys.time()))
      log<<-log %>% rbind(Conn_Flag)
      
      start=Sys.time()
      #Sustitucion de fecha en el query
      query=queryRoot
      query=mgsub(patt,repl,query)
      #Ejecuta el query y guarda en el log la hora
      tmp<<-dbFetch(dbSendQuery(con,query))
      #---------------------------------------------------------
      Dwnl_Flag=print(paste0("Query for",paste(repl,collapse = " : ")," Sent"))
      log<<-log %>% rbind(Dwnl_Flag)
      end=Sys.time()
      #mide la duracion del query
      Elaps_Flag   = print(paste0("elapsed ",-floor(difftime(start,end,unit="secs")/60.0)," Minutes ",round(as.numeric(difftime(start,end,unit="secs")) %% 60)," Seconds"))
      T.Elaps_Flag = print(paste0("Total elapsed : ",-floor(difftime(stt,end,unit="secs")/60.0)))
      print("----------------------------------------------------------------")
      log<<-log %>% rbind(Elaps_Flag) %>% rbind(T.Elaps_Flag)
      dbDisconnect(con)
      trye<<-1;
    },
    error=function(e){
      
      if(trye>maxTry){
        ErrorSkip_Flag=print(paste0("The day: ",paste(repl,collapse = " : ")," was skipped at time ",Sys.time()," after ",trye," Retries"));
        log<<-log %>% rbind(ErrorSkip_Flag);
        trye<<-1;
        return;
      }
      else{
        
        Error_Flag=print(paste0("Error ocurred in day: ",paste(repl,collapse = " : ")," at time ",Sys.time()," Retry : ",trye))
        log<<-log %>% rbind(Error_Flag)
        trye<<-trye+1
        print(paste0("waiting for ",sleep.time," Secs"))
        Sys.sleep(sleep.time)
        fun(repl,queryRoot,patt)
      }
    }
  )
  return (tmp %>% mutate(BD=paste(repl,collapse = " : ")))
}

#Paid_Convert= function (x) {c(as.character(as.Date(paste(year(x)-1,month(x)+1,1,sep="-"))),as.character(x),slob)}


#output_path="D:/Repositorio_Generales/Asistencia_Domiciliaria"

csv_Write = function(x) {x %>%fwrite(paste(output_path,paste0(.$Rpt[1],".csv"),sep="/")) }



#stt=Sys.time()
#log =paste0("Log started at time : ",stt)
##EEOMS = lapply(EOMs,Paid_Convert)
##tsta2k=lapply(EEOMS,fun,queryRoot=query,patt=pattern)
#tsta2k=lapply(EOMs,fun,queryRoot=blok_8,patt=pattern)
#
#
#
#tak = fun(repl=c("2019-01-31"),queryRoot = query,patt = pattern)
#
#lapply(tsta2k,csv_Write)
#
#Siniestros = bind_rows(tsta2)
#
#
#tst=dbFetch(dbSendQuery(con,query))