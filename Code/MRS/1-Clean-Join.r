Sys.setenv(SPARK_HOME="/usr/hdp/current/spark2-client")

library(sparklyr)
library(dplyr)

cc <- rxSparkConnect(interop = "sparklyr",
                     reset = T,
                     consoleOutput = TRUE 
                     # numExecutors = 4,
                     # executorCores = 8,
                     # executorMem = "4g"
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


# Join --------------------------------------------------------------------


# Select desired columns from the flight data. 

varsToKeep <- c("ArrDel15", "Year", "Month", "DayOfMonth", 
                "DayOfWeek", "Carrier", "OriginAirportID", 
                "DestAirportID", "CRSDepTime", "CRSArrTime")

airlineDF <- select_(airlineDF, .dots = varsToKeep)


airlineDF <- airlineDF %>% mutate(CRSDepTime = floor(CRSDepTime / 100))

# Rename Weather Columns --------------------------------------------------


weatherDF <- rename(weatherDF,
                    OriginAirportID = AirportID,
                    Year = AdjustedYear,
                    Month = AdjustedMonth,
                    DayOfMonth = AdjustedDay,
                    CRSDepTime = AdjustedHour)


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

originDF <- originDF %>% sdf_register("flightsweatherorigin")
tbl_cache(sc, "flightsweatherorigin")

#######################################################
# Join airline data with weather at Destination Airport
#######################################################

weatherSummary <- rename(weatherSummary,
                       DestAirportID = OriginAirportID)
                       

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

