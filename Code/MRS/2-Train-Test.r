setwd("/home/sshuser/SparkMLADS/Code/MRS")
source("SetComputeContext.r")

# For local compute context, skip the following line
#startRxSpark()


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

# rxFastTrees

trainXDF <- RxXdfData( file.path(dataDir, "trainXDF" ))
rxDataStep( inData = trainDS, outFile = trainXDF, overwrite = T )

testXDF <- RxXdfData( file.path(dataDir, "testXDF" ))
rxDataStep( inData = testDS, outFile = testXDF, overwrite = T )


rxSetComputeContext("local")
#rxGetInfo(trainXDF, getVarInfo = T, numRows = 3)

t2 <- system.time(
  fastTreesModel <- rxFastTrees(formula, data = trainXDF) #DROPS DATA IF trainDS is RxHiveData
) # 98 sec

unlink(file.path("/tmp", "trainXDF"), recursive = T)
rxHadoopCopyToLocal(file.path(dataDir, "trainXDF" ), "/tmp")
trainXDFLocal <- RxXdfData( file.path("/tmp", "trainXDF"), fileSystem = RxNativeFileSystem())

t2a <- system.time(
  fastTreesModelLocal <- rxFastTrees(formula, data = trainXDFLocal) #DROPS DATA IF trainDS is RxHiveData
) # 18 sec

fastTreesModel
summary(fastTreesModel)

fastTreesPredict <- RxXdfData(file.path(dataDir, "fastTreesPredictSubset"))

# Can we parallelize this with rxExecBy ?
rxPredict(fastTreesModel, data = testXDF, outData = fastTreesPredict,  #DROPS DATA IF testDS is RxHiveData
          extraVarsToWrite = c("ArrDel15"),
          overwrite = TRUE)

rxGetInfo(fastTreesPredict, getVarInfo = T, numRows = 3)
# File name: /share/fastTreesPredictSubset
# Number of composite data files: 1
# Number of observations: 950959
# Number of variables: 4
# Number of blocks: 200
# Compression type: none
# Variable information:
#   Var 1: ArrDel15, Type: numeric, Low/High: (0.0000, 1.0000)
# Var 2: PredictedLabel
# 2 factor levels: 0 1
# Var 3: Score.1, Type: numeric, Storage: float32, Low/High: (-9.1222, 7.4529)
# Var 4: Probability.1, Type: numeric, Storage: float32, Low/High: (0.0254, 0.9517)
# Data (3 rows starting with row 1):
#   ArrDel15 PredictedLabel   Score.1 Probability.1
# 1        0              0 -4.517016     0.1410245
# 2        1              0 -2.790913     0.2466861
# 3        0              0 -4.996718     0.1193408

# Calculate ROC and Area Under the Curve (AUC).

fastTreesRoc <- rxRoc("ArrDel15", "Probability.1", fastTreesPredict)
fastTreesAuc <- rxAuc(fastTreesRoc)
# 0.6296631

plot(fastTreesRoc)

save(fastTreesModel, file = "fastTreesModelSubset.RData")

# rxFastTreesEnsemble

rxSetComputeContext(cc)

#trainers <- list(fastTrees(), fastTrees(numTrees = 60), fastTrees(learningRate = 0.1))
trainers <- list(fastTrees())

#rxSetComputeContext("localpar")
#startRxSpark()

fastTreesEnsembleModelTime <- system.time(
  fastTreesEnsembleModel <- rxEnsemble(formula, data = trainXDF,
    type = "binary", trainers = trainers, modelCount = 4, splitData = TRUE)
) # If using hive: RxSparkData is only supported in RxSpark() with splitData = TRUE
# 190.350 sec

# Try for hive:
# fastTreesEnsembleModelTime <- system.time(
#   fastTreesEnsembleModel <- rxEnsemble(formula, data = trainDS,
#     type = "binary", trainers = trainers, 
#     randomSeed = 111        
#     , replace = FALSE
#     , modelCount = 4
#     , sampRate = 0.75
#     , combineMethod = NULL 
#     , splitData = TRUE)
# )

fastTreesEnsembleModel
summary(fastTreesEnsembleModel)

fastTreesEnsemblePredict <- RxXdfData(file.path(dataDir, "fastTreesEnsemblePredictSubset"))

# Runs locally - Can we parallelize this with rxExecBy ?
rxPredict(fastTreesEnsembleModel, data = testXDF, outData = fastTreesEnsemblePredict,
          extraVarsToWrite = c("ArrDel15"),
          overwrite = TRUE)

rxGetInfo(fastTreesEnsemblePredict, getVarInfo = T, numRows = 3)
# File name: /share/fastTreesEnsemblePredictSubset
# Number of composite data files: 1
# Number of observations: 950959
# Number of variables: 4
# Number of blocks: 200
# Compression type: none
# Variable information:
#   Var 1: ArrDel15, Type: numeric, Low/High: (0.0000, 1.0000)
# Var 2: PredictedLabel
# 2 factor levels: 0 1
# Var 3: Score.1, Type: numeric, Storage: float32, Low/High: (-9.4801, 6.2558)
# Var 4: Probability.1, Type: numeric, Storage: float32, Low/High: (0.0123, 0.9715)
# Data (3 rows starting with row 1):
#   ArrDel15 PredictedLabel   Score.1 Probability.1
# 1        0              0 -5.307837    0.09217435
# 2        1              0 -3.520721    0.19969225
# 3        0              0 -5.039217    0.10412399

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
