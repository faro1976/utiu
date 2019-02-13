#inizializzazione ambiente R-Haddop
library(rmr2)
library(lubridate)
#inizializzazione variabili applicative
PRJ1_HDFSPATH <- "/progetto1/"
YEAR=2015
outfile.root <- "./"
datain <- paste(PRJ1_HDFSPATH,"scontrini.txt",sep="")
dataout <- paste(PRJ1_HDFSPATH,"job2-out",sep="")
outfile.path <- file.path(outfile.root, "incassoMesePerProdotto.txt")
#eventuale rimozione output preceente
system(paste("hadoop fs -rmr ",dataout,sep=""))
startTime <- as.numeric(Sys.time())


#caricamento file prodotti per lookup prezzo in variabile globale
productfile.path <- file.path(outfile.root, "prodotti.txt")
product.df <- read.csv(file=productfile.path, header=F, sep="=", col.names = c("key.product", "price"), dec=".", stringsAsFactors=F)


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

#funzione reduce per calcolo somma occorrenze e incasso totale per prodotto di ciascun mese
reducing = function(key, values) {
  product <- key[[2]]
  prodPrice <- product.df[product.df$key.product == product,]$price
  return(keyval(key, prodPrice * sum(values)))
}

#MapReduce per calcolo incasso totale prodotto per ciascun mese
revenueProductByMonth <- function (input, output) {
  mapreduce(input = input,
            output = output,
            input.format="text",
            map = mapping,
            reduce = reducing)
}

results <- from.dfs(revenueProductByMonth(datain, dataout))
results.df <- as.data.frame(results, stringsAsFactors=F)


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

print(paste("job2 executed in secs",(as.numeric(Sys.time())-startTime)))