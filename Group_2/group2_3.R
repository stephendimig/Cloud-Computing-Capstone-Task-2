#################################################################
##
## File:   group2_1.R
## Author: Stephen Dimig
## Description: 
##
#################################################################

suppressWarnings(suppressMessages(library(dplyr, warn.conflicts=FALSE, quietly=TRUE)))
suppressWarnings(suppressMessages(library(RJDBC, warn.conflicts=FALSE, quietly=TRUE)))


# Command line processing
args <- commandArgs(trailingOnly = TRUE)
X <- args[1]
Y <- args[2] 

# Read data 
cassdrv <- JDBC("org.apache.cassandra.cql.jdbc.CassandraDriver", list.files("/usr/share/cassandra/lib",pattern="jar$",full.names=T))
casscon <- dbConnect(cassdrv, "jdbc:cassandra://localhost:9160/mykeyspace")
query = sprintf("SELECT origin, dest, carrier, arrdelay, total FROM results2_3 WHERE origin='%s' AND dest='%s';", X, Y)
df <- dbGetQuery(casscon, query)
names(df) <- c("origin", "dest", "carrier", "arrdelay", "total")

# Clean data
mydf <- head(df %>%  mutate(arrdelayavg =  arrdelay / total)  %>% arrange(arrdelayavg), 10)

mydf