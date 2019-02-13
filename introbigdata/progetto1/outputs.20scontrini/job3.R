#inizializzazione ambiente R-Haddop
library(rmr2)
library(lubridate)
#inizializzazione variabili applicative
PRJ1_HDFSPATH <- "/progetto1/"
outfile.root <- "./"
datain <- paste(PRJ1_HDFSPATH,"scontrini.txt",sep="")
dataout <- paste(PRJ1_HDFSPATH,"job3-out",sep="")
outfile.path <- file.path(outfile.root, "regoleAssociazione.txt")
kvSep <- "->"
kRowCount = "rowCount"
#eventuale rimozione output precedente
system(paste("hadoop fs -rmr ",dataout,sep=""))
startTime <- as.numeric(Sys.time())


#funzione map per generare chiavi combinazione prodotti per ogni riga, occorrenze singoli prodotti, numero complessivo scontrini
mapping = function(key, lines) {
  vKeys <- c()
  #per ogni riga del blocco
  for (line in lines) {
    fields<-unlist(strsplit(line, ","))
    #calcolo tutte le combinazioni senza ripetizione dei prodotti presi 2 a 2
    items <- fields[-1]
    #eseguo regola associazione solo se almeno due prodotti
    if (length(items) > 1) {
      allSimpleKCombs.df <- combn(items, 2)
      for (i in 1:ncol(allSimpleKCombs.df)) {
        pair = allSimpleKCombs.df[,i]
        item1 <- pair[1]
        item2 <- pair[2]
        #chiave item1-item2
        #vKeys <- append(vKeys, pairKey)
        vKeys <- append(vKeys, paste(item1,item2,sep=kvSep))
        #chiave item2-item1
        vKeys <- append(vKeys, paste(item2,item1,sep=kvSep))
      }
    }
    #calcolo numero complessivo degli scontrini (righe del file)
    vKeys <- append(vKeys, kRowCount)
    #calcolo occorrenze in scontrini dei singoli item
    vKeys <- append(vKeys, fields[-1])
  }
  return(keyval(vKeys,1))
}

#funzione reduce per somma valori per chiave
reducing = function(key, values) {
  return(keyval(key, sum(values)))
}

#MapReduce per calcolo occorrenze combinazione prodotti, singoli prodotti e totale scontrini
associationRule <- function (input, output) {
  mapreduce(input = input,
            output = output,
            input.format="text",
            map = mapping,
            reduce = reducing)
}

results <- from.dfs(associationRule(datain, dataout))
results.df <- as.data.frame(results, stringsAsFactors=F)


#per ciascuna coppia di prodotti (p1,p2):
#(i) la percentuale del numero complessivo di scontrini nei quali i due prodotti compaiono insieme (supporto della regola di associazione p1->p2)
#(ii) la percentuale del numero di scontrini che contengono p1 nei quali compare anche p2 (confidenza della regola di associazione p1->p2)
#pane,latte,30%, 4%

rowCount <- results.df[results.df$key==kRowCount,][2]
itemPairs.df <- subset(results.df, grepl(kvSep, results.df$key))
itemPairs.df <- itemPairs.df[order(itemPairs.df$key),]  #order by key
items.df <- subset(results.df, !(grepl(kvSep, results.df$key)))


cat("", file=outfile.path) #crea/sovrascrivi output file
#per ogni coppia di prodotti calcolo supporto e confidenza
for (i in 1:nrow(itemPairs.df)) {
  pairKey <- itemPairs.df[i,1]
  items <- unlist(strsplit(pairKey, kvSep))
  pairCount <- itemPairs.df[i,2]
  #supporto   = tx contenenti (itemA && itemB) / totale tx
  support <- pairCount/rowCount
  #confidenza = tx contenenti (itemA && itemB) / tx contenenti itemA
  confidence <- pairCount/(items.df[items.df$key==items[1],][[2]])
  pairCount.str <- sprintf("%s, %s, %%%.2f, %%%.2f", items[1], items[2], support*100, confidence*100)
  cat(pairCount.str, file=outfile.path, append=T, sep="\n")
}

print(paste("job3 executed in secs",(as.numeric(Sys.time())-startTime)))