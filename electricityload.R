
  rm(list=ls(all=T)) # this just removes everything from memory

# ETL in R

# Extract and load the last few days


#http://mis.nyiso.com/public/csv/pal/20180624pal.csv

#http://mis.nyiso.com/public/csv/pal/20201107pal.csv


load_url<-'http://mis.nyiso.com/public/csv/pal/'
assigned_zone<-'NORTH' 
run.monthly<-T
out_path<-'C:/Users/CSUFTitan/Desktop/570/hari'
months<-seq(as.Date("2020/10/01"), by = "month", length.out = 1) # months of 2020
months<-rev(months)
zipped_load_urls<-paste(load_url,gsub('-','',months),'pal_csv.zip',sep='') #urls to process

# Open connection
require(RPostgreSQL)
pg = dbDriver("PostgreSQL")
conn = dbConnect(drv=pg
                 ,user="postgres"
                 ,password="hari"
                 ,host="localhost"
                 ,port=5432
                 ,dbname="electricitymarket"
)


for(zipped_url in zipped_load_urls){
  if(!run.monthly) break()
  #zipped_url<-zipped_load_urls[1]
  temp_file<-paste(out_path,'/temp.zip',sep="")
  download.file(zipped_url,temp_file) # download archive to a temp file
  unzip(zipfile = temp_file, exdir = out_path) #extract from archive
  file.remove(temp_file) # delete temp file
  
  csvs<-rev(list.files(out_path,full.names = T))
  
  for(csv in csvs){
    #csv<-csvs[1]
    load_csv<-na.omit(read.csv(file=csv))
    #load_csv$Time.Stamp<-as.character(as.POSIXct(load_csv$Time.Stamp,format="%m/%d/%Y %H:%M:%S"))
    
    cat("Processing",csv,"...")
    t0<-Sys.time()
    for(k in 1:nrow(load_csv)){
      #k<-1
      stm<-paste(
        'INSERT INTO load VALUES ('
        ,"'",load_csv$Time.Stamp[k],"',"
        ,"'",load_csv$Time.Zone[k],"',"
        ,"'",load_csv$Name[k],"',"
        ,load_csv$PTID[k],","
        #,load_csv$Load[k],");"
        ,load_csv$Load[k],") ON CONFLICT (time_stamp, time_zone, ptid) DO NOTHING;"
        ,sep=""
      )
      
      result<-dbSendQuery(conn,stm) # you can inspect the results here
      
      #dbGetQuery(conn,stm)
    } # of for(k)
    t1<-Sys.time()
    file.remove(csv)
    cat('done after',(t1-t0),'s.\n')
    
  } # of for(csv)
  
} # of for(url)

# Close db connection
dbDisconnect(conn)



# Restore point for the database electricitymarket2.backup

# Forecast average hourly load for the next 24 hours ----------------------

# We will now use the data extracted transformed and loaded from NYISO
# We will consider hourly average of total (all zones) load for a time range

# set up time range
from_dt<-'2020-10-01 00:00:00'
to_dt<-'2020-10-31 23:59:59'

#build a a query

qry<-paste(
  "SELECT date_trunc('hour',time_stamp) as ymdh, AVG(total_load) as avg_load
  FROM 
  (SELECT time_stamp, time_zone, SUM(load) as total_load
  FROM load",
  " WHERE time_stamp BETWEEN '",from_dt,"' AND '",to_dt,"' AND node_name LIKE '",assigned_zone,"'",
  " GROUP BY time_stamp, time_zone) TL
  GROUP BY date_trunc('hour',time_stamp)
  ORDER BY ymdh;",sep=""
)

#retrieve from db (we will use the writer role to read - it has all rights)


# Open connection
require(RPostgreSQL)
pg = dbDriver("PostgreSQL")
conn = dbConnect(drv=pg
                 ,user="postgres"
                 ,password="hari"
                 ,host="localhost"
                 ,port=5432
                 ,dbname="electricitymarket"
)

hrly_load<-dbGetQuery(conn,qry) #retrieve data

dbDisconnect(conn) # close connection
#check
head(hrly_load)
tail(hrly_load)
nrow(hrly_load)

#make univariate
rownames(hrly_load)<-hrly_load$ymdh
hrly_load$ymdh<-NULL

#check
head(hrly_load)

#plot the last 7 days (24*7 hours)
plot(tail(hrly_load$avg_load,24*29),type='l')

#make it a time series object
require(xts)
hrly_load.xts<-as.xts(hrly_load)

#better plot (install ggplot2)
require(ggplot2)
ggplot(hrly_load.xts, aes(x = Index, y = avg_load)) + geom_line()

#smart solution (use PerformanceAnalytics)
require(PerformanceAnalytics)
chart.TimeSeries(hrly_load.xts)

#interactive time-series plot (install dygraphs)
require(dygraphs)
dygraph(hrly_load.xts)%>%
  dyAxis("x", label = "Date")%>%
  dyAxis("y", label = "Average Hourly Load")

#forecast the next 24 hours
require(forecast) #did you install this package?

#use a very powerful technique (automatic arima) to fit model
aa<-auto.arima(hrly_load,stepwise = F)
summary(aa)

#forecast
fcst<-forecast(aa,24)
plot(fcst)

#YOURTURN: try fitting without stepwise = F

#how accurate was this forecast?
#load the historical data for the day following the last day

new_from_dt<-as.character(as.POSIXct(to_dt)+1)
new_to_dt<-as.character(as.POSIXct(to_dt)+3600*25)

new_qry<-paste(
  "SELECT date_trunc('hour',time_stamp) as ymdh, AVG(total_load) as avg_load
  FROM 
  (SELECT time_stamp, time_zone, SUM(load) as total_load
  FROM load",
  " WHERE time_stamp BETWEEN '",new_from_dt,"' AND '",new_to_dt,"' AND node_name LIKE '",assigned_zone,"'",
  " GROUP BY time_stamp, time_zone) TL
  GROUP BY date_trunc('hour',time_stamp)
  ORDER BY ymdh;",sep=""
)

conn = dbConnect(drv=pg
                 ,user="postgres"
                 ,password="hari"
                 ,host="localhost"
                 ,port=5432
                 ,dbname="electricitymarket"
)

new_hrly_load<-dbGetQuery(conn,new_qry) #retrieve data
rownames(new_hrly_load)<-new_hrly_load$ymdh
new_hrly_load$ymdh<-NULL

dbDisconnect(conn) # close connection
#check
head(new_hrly_load)
plot(new_hrly_load$avg_load)

#accuracy
accuracy<-new_hrly_load
accuracy$fcst<-fcst$mean # mean is the actual forecast
head(accuracy)

#what is the mean absolute error (MAE) in MW?
accuracy$E<-accuracy$avg_load-accuracy$fcst
accuracy$AE<-abs(accuracy$E)
mae<-mean(accuracy$AE)

#mean absolute percentage error (MAPE)
accuracy$PE<-accuracy$E/accuracy$avg_load
accuracy$APE<-abs(accuracy$PE)
mape<-mean(accuracy$APE)

#plot 
plot(accuracy$avg_load,type='l',xlab=NA,ylab=NA)
par(new=T)
plot(accuracy$fcst,col='blue',xlab=NA,ylab=NA)


require(PerformanceAnalytics)
chart.TimeSeries(accuracy[,c('avg_load','fcst')],legend.loc='bottomright')
chart.TimeSeries(accuracy[,c('E'),drop=F],legend.loc='bottomright')


dygraph(accuracy[,c('avg_load','fcst')])



