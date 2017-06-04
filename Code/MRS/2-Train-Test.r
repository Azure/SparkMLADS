setwd("/home/sshuser/SparkMLADS/Code/MRS")
source("SetComputeContext.r")


################################################
# Split out Training and Test Datasets
################################################

# split out the training data

airWeatherTrainDF <- airWeatherDF %>% filter(Year == 2011) 
airWeatherTrainDF <- airWeatherTrainDF %>% sdf_register("flightsweathertrain")

# split out the testing data

airWeatherTestDF <- airWeatherDF %>% filter(Year == 2012) 
airWeatherTestDF <- airWeatherTestDF %>% sdf_register("flightsweathertest")


# Create ScaleR data source objects

colInfo <- list(
  Year = list(type="factor"),
  Month = list(type="factor"),
  DayOfMonth = list(type="factor"),
  DayOfWeek = list(type="factor"),
  Carrier = list(type="factor"),
  OriginAirportID = list(type="factor"),
  DestAirportID = list(type="factor")
)

finalData <- RxHiveData(table = "flightsweather", colInfo = colInfo)
colInfoFull <- rxCreateColInfo(finalData)

trainDS <- RxHiveData(table = "flightsweathertrain", colInfo = colInfoFull)
testDS <- RxHiveData(table = "flightsweathertest", colInfo = colInfoFull)


################################################
# Train and Test a Logistic Regression model
################################################

formula <- as.formula(ArrDel15 ~ Month + DayOfMonth + DayOfWeek + Carrier + OriginAirportID + 
                        DestAirportID + CRSDepTime + CRSArrTime + RelativeHumidityOrigin + 
                        AltimeterOrigin + DryBulbCelsiusOrigin + WindSpeedOrigin + 
                        VisibilityOrigin + DewPointCelsiusOrigin + RelativeHumidityDest + 
                        AltimeterDest + DryBulbCelsiusDest + WindSpeedDest + VisibilityDest + 
                        DewPointCelsiusDest
)

# Use the scalable rxLogit() function

system.time(
logitModel <- rxLogit(formula, data = trainDS)
) # 87 sec
options(max.print = 10000)
base::summary(logitModel)

# Predict over test data (Logistic Regression).

logitPredict <- RxXdfData(file.path(dataDir, "logitPredictSubset"))

# Use the scalable rxPredict() function

rxPredict(logitModel, data = testDS, outData = logitPredict,
          extraVarsToWrite = c("ArrDel15"),
          type = 'response', overwrite = TRUE)

# Calculate ROC and Area Under the Curve (AUC).

logitRoc <- rxRoc("ArrDel15", "ArrDel15_Pred", logitPredict)
logitAuc <- rxAuc(logitRoc)
# 0.619

plot(logitRoc)

save(logitModel, file = "logitModelSubset.RData")


#####################################
# rxEnsemble of fastTrees
#####################################

trainXDF <- RxXdfData( file.path(dataDir, "trainXDF" ))
rxDataStep( inData = trainDS, outFile = trainXDF, overwrite = T )

testXDF <- RxXdfData( file.path(dataDir, "testXDF" ))
rxDataStep( inData = testDS, outFile = testXDF, overwrite = T )

trainers <- list(fastTrees())

fastTreesEnsembleModelTime <- system.time(
  fastTreesEnsembleModel <- rxEnsemble(formula, data = trainXDF,
    type = "binary", trainers = trainers, modelCount = 4, splitData = TRUE)
)

fastTreesEnsembleModel
summary(fastTreesEnsembleModel)

fastTreesEnsemblePredict <- RxXdfData(file.path(dataDir, "fastTreesEnsemblePredictSubset"))

rxSetComputeContext("local")
# Runs locally - Can we parallelize this with rxExecBy ?
fastTreesEnsemblePredictTime <- system.time(
  rxPredict(fastTreesEnsembleModel, data = testXDF, outData = fastTreesEnsemblePredict,
          extraVarsToWrite = c("ArrDel15"),
          overwrite = TRUE)
)

# Calculate ROC and Area Under the Curve (AUC).

fastTreesEnsembleRoc <- rxRoc("ArrDel15", "Probability.1", fastTreesEnsemblePredict)
fastTreesEnsembleAuc <- rxAuc(fastTreesEnsembleRoc)
# 0.6314349

plot(fastTreesEnsembleRoc)

save(fastTreesEnsembleModel, file = "fastTreesEnsembleModelSubset.RData")

#####################################
# TODO: H2O interop
#####################################


# For local compute context, skip the following line
rxSparkDisconnect(cc)
