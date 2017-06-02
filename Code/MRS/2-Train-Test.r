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

t2 <- system.time(
  fastTreesModel <- rxFastTrees(formula, data = trainDS) #DROPS DATA IF trainDS is RxHiveData
)
# 13.6 sec

fastTreesModel
summary(fastTreesModel)

fastTreesPredict <- RxXdfData(file.path(dataDir, "fastTreesPredictSubset"))

rxPredict(fastTreesModel, data = testDS, outData = fastTreesPredict,  #DROPS DATA IF testDS is RxHiveData
          extraVarsToWrite = c("ArrDel15"),
          overwrite = TRUE)

rxGetInfo(fastTreesPredict, getVarInfo = T, numRows = 3)
# File name: C:\Users\marinch\OneDrive - Microsoft\Conferences\MLADS June 2017\Code\2017-05-04\delayDataLarge\fastTreesPredictSubset.xdf 
# Number of observations: 402819 
# Number of variables: 4 
# Number of blocks: 200 
# Compression type: none 
# Variable information: 
#   Var 1: ArrDel15, Type: integer, Low/High: (0, 1)
# Var 2: PredictedLabel
# 2 factor levels: 0 1
# Var 3: Score.1, Type: numeric, Storage: float32, Low/High: (-9.5904, 6.1169)
# Var 4: Probability.1, Type: numeric, Storage: float32, Low/High: (0.0211, 0.9203)
# Data (3 rows starting with row 1):
#   ArrDel15 PredictedLabel   Score.1 Probability.1
# 1        0              0 -6.201247    0.07723664
# 2        0              0 -6.456911    0.07025596
# 3        0              0 -7.068470    0.05586200

# Calculate ROC and Area Under the Curve (AUC).

fastTreesRoc <- rxRoc("ArrDel15", "Probability.1", fastTreesPredict)
fastTreesAuc <- rxAuc(fastTreesRoc)
# 0.6530

plot(fastTreesRoc)

save(fastTreesModel, file = "fastTreesModelSubset.RData")

# rxFastTreesEnsemble

trainers <- list(fastTrees(), fastTrees(numTrees = 60), fastTrees(learningRate = 0.1))

#rxSetComputeContext("localpar")
#startRxSpark()

fastTreesEnsembleModelTime <- system.time(
  fastTreesEnsembleModel <- rxEnsemble(formula, data = trainDS,
    type = "binary", trainers = trainers, replace = T)
) # If using hive: RxSparkData is only supported in RxSpark() with splitData = TRUE
# 64.73 sec

# Try for hive:
fastTreesEnsembleModelTime <- system.time(
  fastTreesEnsembleModel <- rxEnsemble(formula, data = trainDS,
    type = "binary", trainers = trainers, 
    randomSeed = 111        
    , replace = FALSE
    , modelCount = 4
    , sampRate = 0.75
    , combineMethod = NULL 
    , splitData = TRUE)
)

fastTreesEnsembleModel
summary(fastTreesEnsembleModel)

fastTreesEnsemblePredict <- RxXdfData(file.path(dataDir, "fastTreesEnsemblePredictSubset"))

rxPredict(fastTreesEnsembleModel, data = testDS, outData = fastTreesEnsemblePredict,
          extraVarsToWrite = c("ArrDel15"),
          overwrite = TRUE)

rxGetInfo(fastTreesEnsemblePredict, getVarInfo = T, numRows = 3)
# File name: C:\Users\marinch\OneDrive - Microsoft\Conferences\MLADS June 2017\Code\2017-05-04\delayDataLarge\fastTreesEnsemblePredictSubset.xdf 
# Number of observations: 402819 
# Number of variables: 4 
# Number of blocks: 200 
# Compression type: none 
# Variable information: 
#   Var 1: ArrDel15, Type: integer, Low/High: (0, 1)
# Var 2: PredictedLabel
# 2 factor levels: 0 1
# Var 3: Score.1, Type: numeric, Storage: float32, Low/High: (-9.1441, 5.7202)
# Var 4: Probability.1, Type: numeric, Storage: float32, Low/High: (0.0188, 0.9572)
# Data (3 rows starting with row 1):
#   ArrDel15 PredictedLabel   Score.1 Probability.1
# 1        0              0 -6.185685    0.07252729
# 2        0              0 -6.719369    0.05721389
# 3        0              0 -7.188776    0.04630702

# Calculate ROC and Area Under the Curve (AUC).

fastTreesEnsembleRoc <- rxRoc("ArrDel15", "Probability.1", fastTreesEnsemblePredict)
fastTreesEnsembleAuc <- rxAuc(fastTreesEnsembleRoc)
# 0.6560

plot(fastTreesEnsembleRoc)

save(fastTreesEnsembleModel, file = "fastTreesEnsembleModelSubset.RData")

#####################################
# TODO: H2O interop
#####################################


# For local compute context, skip the following line
rxSparkDisconnect(rxGetComputeContext())
