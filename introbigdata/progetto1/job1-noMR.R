#inizializzazione
library(lubridate)
YEAR=2015
outfile.root <- "./"
datain <- file.path(outfile.root, "scontrini.txt")
outfile.path <- file.path(outfile.root, "top5PerMese.noMR.txt")
outList = list()
startTime <- as.numeric(Sys.time())


#lettura file scontrini 
conn <- file(datain,open="r")
lines <- readLines(conn)
for (line in lines){
  fields<-unlist(strsplit(line, ","))
  currDate <- as.Date(fields[[1]],"%Y-%m-%d")
  currYear <- year(currDate)
  if (currYear == YEAR) {
    currMonth <- month(currDate)
    for (field in fields[-1]) {
      key <- paste(currMonth, field, sep="@")
      if (key %in% names(outList)) {
        outList[key] <- outList[[key]] + 1
      } else {
        outList[key] <- 1
      }
    }
  }
}
close(conn)


#popolamento dataframe per ospitare risultati
results.df <- data.frame("key.month"=integer(), "key.product"=character(), "val"=integer(), stringsAsFactors=F)
for(key in names(outList)){
  keys <- unlist(strsplit(key, "@"))
  val <- outList[[key]]
  results.df <- rbind(results.df, data.frame("key.month" = as.numeric(keys[1]), "key.product" = keys[2], "val" = val, stringsAsFactors=F))
}
head(results.df, 5)   #stampa prime 5 righe su stdout per debug

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

print(paste("job1-noMR executed in secs",(as.numeric(Sys.time())-startTime)))