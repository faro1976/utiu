#inizializzazione
library(lubridate)
YEAR=2015
outfile.root <- "./"
datain <- file.path(outfile.root, "scontrini.txt")
outfile.path <- file.path(outfile.root, "incassoMesePerProdotto.noMR.txt")
outList = list()
startTime <- as.numeric(Sys.time())


#caricamento file prodotti per lookup prezzo in variabile globale
productfile.path <- file.path(outfile.root, "prodotti.txt")
product.df <- read.csv(file=productfile.path, header=F, sep="=", col.names = c("key.product", "price"), dec=".", stringsAsFactors=F)


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
      prodPrice <- product.df[product.df$key.product == field,]$price
      if (key %in% names(outList)) {
        outList[key] <- outList[[key]] + prodPrice
      } else {
        outList[key] <- prodPrice
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

#per ciascun prodotto, l'incasso totale per quel prodotto di ciascun mese del 2015
#pane 1/2015:12340 2/2015:8530 3/2015:9450 ...
#ordina dataframe per prodotto, mese 
productRevForMont.df <- results.df[order(results.df$key.product,results.df$key.month, decreasing=F),]
cat("", file=outfile.path) #crea/sovrascrivi output file
prod.last <- ""
for (i in 1:nrow(productRevForMont.df)) {
   row = productRevForMont.df[i,]
    if (row[[2]] != prod.last) {
    if (prod.last != "") cat("\n", file=outfile.path, append=T) 
    cat(row[[2]], file=outfile.path, append=T)
    prod.last <- row[[2]]
  }
  productRevForMont.str <- sprintf(" %s/%s:%s", row[[1]], YEAR, row[[3]])
  cat(productRevForMont.str, file=outfile.path, append=T, sep=" ")
}

print(paste("job2-noMR executed in secs",(as.numeric(Sys.time())-startTime)))