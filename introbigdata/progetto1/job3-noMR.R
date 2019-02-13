#inizializzazione
library(lubridate)
YEAR=2015
outfile.root <- "./"
datain <- file.path(outfile.root, "scontrini.txt")
outfile.path <- file.path(outfile.root, "regoleAssociazione.noMR.txt")
outList = list()
kvSep <- "->"
kRowCount = "rowCount"
startTime <- as.numeric(Sys.time())


#lettura file scontrini 
conn <- file(datain,open="r")
lines <- readLines(conn)
for (line in lines){
  #per ogni riga del blocco
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
      key1 <- paste(item1,item2,sep=kvSep)
      #chiave item2-item1
      key2 <- paste(item2,item1,sep=kvSep)
      if (key1 %in% names(outList)) {
        outList[key1] <- outList[[key1]] + 1
      } else {
        outList[key1] <- 1
      }
      if (key2 %in% names(outList)) {
        outList[key2] <- outList[[key2]] + 1
      } else {
        outList[key2] <- 1
      }
    }
  }
  #calcolo numero complessivo degli scontrini (righe del file)
  if (kRowCount %in% names(outList)) {
    outList[kRowCount] <- outList[[kRowCount]] + 1
  } else {
    outList[kRowCount] <- 1
  }
    
  #calcolo occorrenze in scontrini dei singoli item
  for (field in fields[-1]) {
    key <- field
    if (key %in% names(outList)) {
      outList[key] <- outList[[key]] + 1
    } else {
      outList[key] <- 1
    }
  }
}
close(conn)


#popolamento dataframe per ospitare risultati
results.df <- data.frame("key"=character(), "val"=numeric(), stringsAsFactors=F)
for(key in names(outList)){
  val <- outList[[key]]
  results.df <- rbind(results.df, data.frame("key"=key, "val" = val, stringsAsFactors=F))
}
head(results.df, 5)   #stampa prime 5 righe su stdout per debug


#per ciascuna coppia di prodotti (p1,p2):
#(i) la percentuale del numero complessivo di scontrini nei quali i due prodotti compaiono insieme (supporto della regola di associazione p1->p2)
#(ii) la percentuale del numero di scontrini che contengono p1 nei quali compare anche p2 (confidenza della regola di associazione p1->p2)
#pane,latte,30%, 4%

rowCount <- results.df[results.df$key==kRowCount,][2]
itemPairs.df <- subset(results.df, grepl(kvSep, results.df$key))
itemPairs.df <- itemPairs.df[order(itemPairs.df$key),]  #order by key
items.df <- subset(results.df, !(grepl(kvSep, results.df$key)))
head(itemPairs.df,5)

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

print(paste("job3-noMR executed in secs",(as.numeric(Sys.time())-startTime)))