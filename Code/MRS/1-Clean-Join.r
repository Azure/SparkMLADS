Sys.setenv(SPARK_HOME="/usr/hdp/current/spark2-client")

library(sparklyr)
library(dplyr)
#library(tidyverse)

connection_list_tables <- function (sc, includeType = FALSE) 
{
  dbi <- sc
  if (!is.null(dbi)) 
    sort(dplyr:::db_list_tables(dbi))
  else character()
}

assignInNamespace("connection_list_tables",connection_list_tables, ns="sparklyr")


# Configure cluster (D13v2large 56G 8core 400GBdisk) ----------------------

# conf <- sparklyr::spark_config()
# conf$'sparklyr.shell.executor-memory' <- "4g"
# conf$'sparklyr.shell.driver-memory' <- "4g"
# conf$spark.executor.cores <- 4
# conf$spark.executor.memory <- "4G"
# conf$spark.yarn.am.cores  <- 4
# conf$spark.yarn.am.memory <- "4G"
# conf$spark.dynamicAllocation.enabled <- "false"
# conf$spark.default.parallelism <- 8

# Connect to cluster ------------------------------------------------------

# sc <- sparklyr::spark_connect(master = "yarn-client", config = conf)

cc <- rxSparkConnect(interop = "sparklyr",
                     reset = T,
                     consoleOutput = TRUE, 
                     numExecutors = 4,  # FOR HDINSIGHT CLUSTER
                     executorCores = 8, # FOR HDINSIGHT CLUSTER
                     executorMem = "4g" # FOR HDINSIGHT CLUSTER
)

sc <- rxGetSparklyrConnection(cc)


# Load Data to Spark DataFrames -------------------------------------------


airlineDF <- sparklyr::spark_read_csv(sc = sc, 
                                      name = "airline",
                                      path = "/marinch/AirlineSubsetCsv", 
                                      header = TRUE, 
                                      infer_schema = TRUE, 
                                      null_value = "null")

weatherDF <- sparklyr::spark_read_csv(sc = sc, 
                                      name = "weather",
                                      path = "/marinch/WeatherSubsetCsv",
                                      header = TRUE,
                                      infer_schema = TRUE,
                                      null_value = "null")



# Rename Airline Columns --------------------------------------------------


airNames <- colnames(airlineDF)

airNames[airNames == "ARR_DEL15"] <- "ArrDel15"
airNames[airNames == "YEAR"] <- "Year"
airNames[airNames == "MONTH"] <- "Month"
airNames[airNames == "DAY_OF_MONTH"] <- "DayOfMonth"
airNames[airNames == "DAY_OF_WEEK"] <- "DayOfWeek"
airNames[airNames == "UNIQUE_CARRIER"] <- "Carrier"
airNames[airNames == "ORIGIN_AIRPORT_ID"] <- "OriginAirportID"
airNames[airNames == "DEST_AIRPORT_ID"] <- "DestAirportID"
airNames[airNames == "CRS_DEP_TIME"] <- "CRSDepTime"
airNames[airNames == "CRS_ARR_TIME"] <- "CRSArrTime"

airlineDF <- airlineDF %>% setNames(airNames)


# Join --------------------------------------------------------------------


# Select desired columns from the flight data. 

varsToKeep <- c("ArrDel15", "Year", "Month", "DayOfMonth", 
                "DayOfWeek", "Carrier", "OriginAirportID", 
                "DestAirportID", "CRSDepTime", "CRSArrTime")

airlineDF <- select_(airlineDF, .dots = varsToKeep)


airlineDF <- airlineDF %>% mutate(CRSDepTime = floor(CRSDepTime / 100))

# Rename Weather Columns --------------------------------------------------

weatherNames <- colnames(weatherDF)

weatherNames[weatherNames == "AirportID"] <- "OriginAirportID"
weatherNames[weatherNames == "AdjustedYear"] <- "Year"
weatherNames[weatherNames == "AdjustedMonth"] <- "Month"
weatherNames[weatherNames == "AdjustedDay"] <- "DayOfMonth"
weatherNames[weatherNames == "AdjustedHour"] <- "CRSDepTime"

weatherDF <- weatherDF %>% setNames(weatherNames)

weatherSummary <- weatherDF %>% 
  group_by(Year, Month, DayOfMonth, CRSDepTime, OriginAirportID) %>% 
  summarise(Visibility = mean(Visibility),
            DryBulbCelsius = mean(DryBulbCelsius),
            DewPointCelsius = mean(DewPointCelsius),
            RelativeHumidity = mean(RelativeHumidity),
            WindSpeed = mean(WindSpeed),
            Altimeter = mean(Altimeter))

# weatherSummary <- weatherSummary %>% sdf_register("weather_summary")
# tbl_cache(sc, "weather_summary")

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

originDF <- originDF %>% sdf_register("flightsweatherorigin")
tbl_cache(sc, "flightsweatherorigin")

#######################################################
# Join airline data with weather at Destination Airport
#######################################################

weatherSummary <- rename(weatherSummary,
                       DestAirportID = OriginAirportID)
                       
# weatherSummaryNames <- colnames(weatherSummary)
# weatherSummaryNames[weatherSummaryNames == "OriginAirportID"] <- "DestAirportID"
# 
# weatherSummary <- weatherSummary %>% setNames(weatherSummaryNames)

destDF <- left_join(x = originDF,
                    y = weatherSummary)


# Rename Columns and Drop Reduncies ---------------------------------------

airWeatherDF <- rename(destDF,
                       VisibilityDest = Visibility,
                       DryBulbCelsiusDest = DryBulbCelsius,
                       DewPointCelsiusDest = DewPointCelsius,
                       RelativeHumidityDest = RelativeHumidity,
                       WindSpeedDest = WindSpeed,
                       AltimeterDest = Altimeter)

airWeatherDF <- airWeatherDF %>% sdf_register("flightsweather")
tbl_cache(sc, "flightsweather")

library(DBI)
dbGetQuery(sc, "SELECT ArrDel15, COUNT(*) FROM flightsweather GROUP BY arrDel15")
dbGetQuery(sc, "SELECT OriginAirportID, DestAirportID, VisibilityOrigin, VisibilityDest FROM flightsweather LIMIT 10")

