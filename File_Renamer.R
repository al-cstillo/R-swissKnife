library(rstudioapi)
#seleccionar folder donde s encuentran los archivos a cambia el nombre
carpeta= selectDirectory(caption="Carpeta contenedora de archivos",label="Seleccionar",path=NULL)
archivo=list.files(carpeta,full.names = TRUE)
target=showPrompt("Target","Frase que desea cambiar")
goal=showPrompt("Goal","Frase que desea colocar")
sapply(archivo,FUN=function(path){
  path.new=sub(pattern =tolower(target),replacement = goal,tolower(basename(path)))
  path.new=paste(carpeta,sep="/",path.new)
  file.rename(from=path,to=path.new)
})
#rm(list=ls())
data.data=list()
for (i in (1:length(files))){
  data.data[[i]]=read.xlsx(files[i],sheetIndex = 1,startRow = 4)
  print(cat("Finished reading ",i))
}