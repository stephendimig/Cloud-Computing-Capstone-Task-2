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

# Read data 
cassdrv <- JDBC("org.apache.cassandra.cql.jdbc.CassandraDriver", list.files("/usr/share/cassandra/lib",pattern="jar$",full.names=T))
casscon <- dbConnect(cassdrv, "jdbc:cassandra://localhost:9160/mykeyspace")
query = sprintf("SELECT origin, carrier, depdelay, total FROM results2_1 WHERE origin='%s';", X)
df <- dbGetQuery(casscon, query)
names(df) <- c("origin", "carrier", "depdelay", "total")

# Clean data
mydf <- head(df %>%  mutate(depdelayavg =  depdelay / total)  %>% arrange(depdelayavg), 10)

mydf