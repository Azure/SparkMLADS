setwd("/home/sshuser/SparkMLADS/Code/MRS")
source("SetComputeContext.r")

library(sparklyr)
library(dplyr)

cc <- rxSparkConnect(interop = "sparklyr",
                     reset = T,
                     consoleOutput = TRUE,
                     extraSparkConfig = "--conf spark.dynamicAllocation.enabled=false" # for H2O
                     # numExecutors = 4,
                     # executorCores = 8,
                     # executorMem = "4g"
)

sc <- rxGetSparklyrConnection(cc)


################################################
# Specify the data sources
################################################


airlineDF <- sparklyr::spark_read_csv(sc = sc, 
                                      name = "airline",
                                      path = "hdfs://mycluster/share/Air2009to2012CSV", 
                                      header = TRUE, 
                                      infer_schema = FALSE, # Avoids parsing error
                                      null_value = "null")

weatherDF <- sparklyr::spark_read_csv(sc = sc, 
                                      name = "weather",
                                      path = "hdfs://mycluster/share/Weather",
                                      header = TRUE,
                                      infer_schema = TRUE,
                                      null_value = "null")


################################################
# Transform the data
################################################


airlineDF <- rename(airlineDF,
                    ArrDel15 = ARR_DEL15,
                    Year = YEAR,
                    Month = MONTH,
                    DayOfMonth = DAY_OF_MONTH,
                    DayOfWeek = DAY_OF_WEEK,
                    Carrier = UNIQUE_CARRIER,
                    OriginAirportID = ORIGIN_AIRPORT_ID,
                    DestAirportID = DEST_AIRPORT_ID,
                    CRSDepTime = CRS_DEP_TIME,
                    CRSArrTime = CRS_ARR_TIME
)


# Keep only the desired columns from the flight data 

varsToKeep <- c("ArrDel15", "Year", "Month", "DayOfMonth", 
                "DayOfWeek", "Carrier", "OriginAirportID", 
                "DestAirportID", "CRSDepTime", "CRSArrTime")

airlineDF <- select_(airlineDF, .dots = varsToKeep)


# Round down scheduled departure time to full hour

airlineDF <- airlineDF %>% mutate(CRSDepTime = floor(CRSDepTime / 100))


weatherDF <- rename(weatherDF,
                    OriginAirportID = AirportID,
                    Year = AdjustedYear,
                    Month = AdjustedMonth,
                    DayOfMonth = AdjustedDay,
                    CRSDepTime = AdjustedHour)


# Average the weather readings by hour

weatherSummary <- weatherDF %>% 
  group_by(Year, Month, DayOfMonth, CRSDepTime, OriginAirportID) %>% 
  summarise(Visibility = mean(Visibility),
            DryBulbCelsius = mean(DryBulbCelsius),
            DewPointCelsius = mean(DewPointCelsius),
            RelativeHumidity = mean(RelativeHumidity),
            WindSpeed = mean(WindSpeed),
            Altimeter = mean(Altimeter))


#######################################################
# Join airline data with weather at Origin Airport
#######################################################

originDF <- left_join(x = airlineDF,
                      y = weatherSummary)

originDF <- originDF %>% rename(VisibilityOrigin = Visibility,
                                DryBulbCelsiusOrigin = DryBulbCelsius,
                                DewPointCelsiusOrigin = DewPointCelsius,
                                RelativeHumidityOrigin = RelativeHumidity,
                                WindSpeedOrigin = WindSpeed,
                                AltimeterOrigin = Altimeter)


#######################################################
# Join airline data with weather at Destination Airport
#######################################################

weatherSummary <- rename(weatherSummary,
                       DestAirportID = OriginAirportID)
                       
destDF <- left_join(x = originDF,
                    y = weatherSummary)

airWeatherDF <- rename(destDF,
                       VisibilityDest = Visibility,
                       DryBulbCelsiusDest = DryBulbCelsius,
                       DewPointCelsiusDest = DewPointCelsius,
                       RelativeHumidityDest = RelativeHumidity,
                       WindSpeedDest = WindSpeed,
                       AltimeterDest = Altimeter)


#######################################################
# Register the joined data as a Spark SQL/Hive table
#######################################################
  
airWeatherDF <- airWeatherDF %>% sdf_register("flightsweather")
tbl_cache(sc, "flightsweather")


#######################################################
# The table of joined data can be queried using SQL
#######################################################

# Count the number of rows
tbl(sc, sql("SELECT COUNT(*) FROM flightsweather"))

# Count each distinct value in the ArrDel15 column
tbl(sc, sql("SELECT ArrDel15, COUNT(*) FROM flightsweather GROUP BY ArrDel15"))
