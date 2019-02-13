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


#funzione map per trovare numero occorrenze prodotti per mese
mapping = function(key, lines) {
  months<-list()
  products<-list()
  for (line in lines) {
    fields<-unlist(strsplit(line, ","))
    currDate <- as.Date(fields[1],"%Y-%m-%d")
    currYear <- year(currDate)
    if (currYear == YEAR) {
      currMonth <- month(currDate)
      months <- c(months,lapply(1:(length(fields)-1), function(x) currMonth))
      products <- c(products,fields[-1])
    }
  }
  return(keyval(data.frame("month"=unlist(months),"product"=unlist(products), stringsAsFactors=F),1))  
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
