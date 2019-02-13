#inizializzazione ambiente R-Haddop
library(rmr2)
library(lubridate)
#inizializzazione variabili applicative
PRJ1_HDFSPATH <- "/progetto1/"
YEAR=2015
outfile.root <- "./"
datain <- paste(PRJ1_HDFSPATH,"scontrini.txt",sep="")
dataout <- paste(PRJ1_HDFSPATH,"job1-out",sep="")
outfile.path <- file.path(outfile.root, "top5PerMese.txt")
#eventuale rimozione output preceente
system(paste("hadoop fs -rmr ",dataout,sep=""))
startTime <- as.numeric(Sys.time())
#filerob<- file.path("/", "mr.rob")
filerob<- "mr.rob"

#funzione map per trovare numero occorrenze prodotti per mese
mapping = function(key, lines) {
  cat(paste("rob1",as.character(Sys.time()),sep=":"), file=filerob, append=T)
  cat("\n", file=filerob, append=T)
  
  lines.tot <- length(lines)
  df.max<-lines.tot*5
  #dfKeys <- data.frame( "month" = integer(), "product" = character())
  dfKeys <- data.frame( "month" = integer(df.max), "product" = character(df.max))
  #dfKeys <- list()
  cat(paste("rob2",as.character(Sys.time()),sep=":"), file=filerob, append=T)
  cat("\n", file=filerob, append=T)  
  
  #dfKeys<-as.data.frame(t(sapply(lines, function(line) {
  #i<-0
  months<-list()
  products<-list()
  for (line in lines) {
   #cat(paste("robline",line,sep="\n"), file=filerob, append=T)
    fields<-unlist(strsplit(line, ","))
    currDate <- as.Date(fields[1],"%Y-%m-%d")
    currYear <- year(currDate)
    if (currYear == YEAR) {
      currMonth <- month(currDate)
      #dfKeys<-rbind(dfKeys, data.frame(month=currMonth, product=fields[-1], stringsAsFactors=F))
      #dfKeys <- c(dfKeys, c(currMonth, fields[-1]))
      #for (field in fields){
      #  dfKeys[i,1]<-currMonth
      #  dfKeys[i,2]<-field
      #  i<i+1
      #}
      months <- c(months,lapply(1:(length(fields)-1), function(x) currMonth))
      products <- c(products,fields[-1])
      #return (c(currMonth, fields[-1]))
    }
  }

#  })))
  cat(paste("rob3",as.character(Sys.time()),sep=":"), file=filerob, append=T)
  cat("\n", file=filerob, append=T)
  #return(keyval(dfKeys,1))
  return(keyval(data.frame("month"=unlist(months),"product"=unlist(products)),1))
}

#funzione reduce per somma occorrenze trovate
reducing = function(key, values) {
  return(keyval(key, sum(values)))
}

#MapReduce per somma prodotti per mese
sumProductByMonth <- function (input, output) {
  mapreduce(input = input,
            output = output,
            input.format="text",
            map = mapping,
            reduce = reducing)
}

results <- from.dfs(sumProductByMonth(datain, dataout))
results.df <- as.data.frame(results, stringsAsFactors=F)
results
results.df

#per ciascun mese del 2015, i cinque prodotti piu venduti seguiti dal numero complessivo di pezzi venduti
#2015-01: pane 852, latte 753, carne 544, vino 501, pesce 488
cat("", file=outfile.path) #crea/sovrascrivi output file
for (i in 1:12) {
  currMonth.df <- results.df[results.df$key.month==i,]
  if (nrow(currMonth.df) > 0) {
    top5forMonth.str <- sprintf("%04d-%02d: ", YEAR, i)
    cat(top5forMonth.str, file=outfile.path, append=T)
    top5forMonth.df <- head(currMonth.df[order(currMonth.df$val,currMonth.df$key.product,decreasing=T),], 5)
    cat(apply(top5forMonth.df, 1, function(x) paste(x[2], x[3], sep=" ")), file=outfile.path, append=T, sep=", ")
    cat("\n", file=outfile.path, append=T)
  }
}

print(paste("job1 executed in secs",(as.numeric(Sys.time())-startTime)))
