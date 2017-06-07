# install.packages("ggplot2", "tidyr")
# see /home/sshuser/SparkMLADS/Code/MRS/SetComputeContext.r
Sys.setenv(SPARK_HOME="/usr/hdp/current/spark2-client")
setwd("~/SparkMLADS/Code/learning_curves")

## Simulating data

t0 <- Sys.time()
MILLIONS_OF_ROWS <- 1

N <- MILLIONS_OF_ROWS * 1e6
CHUNK_SIZE <- 1e5 # must divide a million evenly

NUM_VARS <- 10
NOISE <- 20
SIMULATE_DATA <- FALSE
RUN_LOCAL <- TRUE
HDINSIGHT <- TRUE

REGRESSION_LEARNERS <- list(
  rxLinMod = list(cube=TRUE) # ,
  # rxBTrees = list(lossFunction="gaussian", learningRate=0.5, maxDepth=10),
  # rxDForest = list(),
  # rxDTree = list()
)

## data simulation functions

GENERATING_COEFFICIENTS <- lapply(1:NUM_VARS, function(i){
  cardinality <- 2^(i)
  gc <- rnorm(cardinality)
  names(gc) <- sprintf("%s%05d", letters[i], 1:cardinality)
  gc
})
names(GENERATING_COEFFICIENTS) <- LETTERS[1:NUM_VARS]

simulate_data <- function(N, gencoef, noise=10){
  sd <- data.frame(lapply(gencoef, function(gc){
    base::sample(names(gc), N, replace=TRUE, prob=length(gc):1)
  }), stringsAsFactors=FALSE)
  col_weights <- lapply(seq_along(sd), function(i){gencoef[[i]][sd[[i]]]})
  sd$y <- Reduce("+", col_weights) + rnorm(N, sd=noise)
  sd
}

dataDir <- "/user/RevoShare"

dpath <- dataDir
for (ddir in c("sshuser", "simdata")){
  dpath <- file.path(dpath, ddir)
  if (!rxHadoopFileExists(dpath)) rxHadoopMakeDir(dpath)
}

data_table <- RxXdfData(file.path(dataDir, "sshuser", "simdata"), fileSystem=RxHdfsFileSystem())

# we'll use 'colInfo' to set the factor levels so they are consistent across chunks
COL_INFO <- c(lapply(GENERATING_COEFFICIENTS, function(gc) list(type="factor", levels=names(gc))), 
              list(y=list(type="numeric")))

if (SIMULATE_DATA){
  for (i in 1:(N /CHUNK_SIZE)) {
    chunk_data <- simulate_data(CHUNK_SIZE, GENERATING_COEFFICIENTS, noise = NOISE)
    rxImport(chunk_data, 
             outFile = data_table, 
             colInfo = COL_INFO,
             append = i>1, overwrite = i==1)
  }
}

print(Sys.time() - t0) # 1.134102 mins

## Examine simulated data

head(data_table, n=15)

metadata <- rxGetInfo(data_table, getVarInfo = TRUE)
metadata

rxSummary( ~ C, data_table)

outcome <- "y"
var_names <- setdiff(names(metadata$varInfo), outcome)
names(var_names) <- var_names

var_names


## Learning curves

source("learning_curve_lib.R")

K_FOLDS <- 3
SALT <- 1
NUM_TSS <- 12

# metadata <- rxGetInfo(data_table, getVarInfo=TRUE)
N <- metadata$numRows
MAX_TSS <- (1 - 1/K_FOLDS) * N # approximate number of cases available for training.
training_fractions <- get_training_set_fractions(10000, MAX_TSS, NUM_TSS)


formula_vec <- sapply(1:length(var_names), function(j){
  vars <- var_names[j:1]
  paste(outcome, paste(vars, collapse="+"), sep=" ~ ")
})

grid_dimensions <- list( model_class=names(REGRESSION_LEARNERS),
                         training_fraction=training_fractions,
                         with_formula=formula_vec[1:10],
                         test_set_kfold_id=1, #:3,
                         KFOLDS=K_FOLDS)

parameter_table <- do.call(expand.grid, c(grid_dimensions, stringsAsFactors=FALSE))

dim(parameter_table)

head(parameter_table, n=15)

parameter_list <- lapply(1:nrow(parameter_table), function(i){
  par <- parameter_table[i,]
  as.list(c(data_table=data_table, par, REGRESSION_LEARNERS[[par$model_class]]))
})

cc <- rxSparkConnect(reset = T,
               consoleOutput = TRUE,
               numExecutors = 8,
               executorCores = 4,
               executorMem = "4g"
)

rxSetComputeContext(cc)

t1 <- Sys.time()
training_results <- rxExec(run_training_fraction,
                           elemArgs = parameter_list,
                           execObjects = c("data_table", "SALT"))

t2 <- Sys.time()
rxSparkDisconnect(cc)

print(t2 - t1) # Time difference of 1.491542 hours

saveRDS(training_results, "training_results.Rds")

error_ids <- grep("^Error", training_results) # 267
parameter_list[error_ids]

library(ggplot2)
library(dplyr)
library(tidyr)

df <- training_results %>% # head(n=100) %>%
  "["(grep("^Error", training_results, invert=TRUE)) %>%
  bind_rows %>%
  gather(error_type, error, training, test) %>%
  mutate(kfold=factor(kfold)) 

gg1 <- df %>% 
  ggplot(aes(x=log10(tss), y=error, col=formula, linetype=error_type, group=interaction(formula, error_type, kfold))) +
  geom_line(size=1.2) +  
  facet_grid(~ model_class) +
  ylab("test RMSE")

gg1 + ggtitle(sprintf("Simulated high-cardinality data"))

gg1 + coord_cartesian(ylim=c(19.95,20.25)) + ggtitle("Close-Up View")

gg2 <- df %>%
  filter(kfold==1, error_type=="test") %>%
  ggplot(aes(x=log10(tss), y=error, col=formula, linetype=error_type, group=interaction(formula, error_type, kfold))) +
  geom_line(size=1.2) +  
  facet_grid(~ model_class) +
  ylab("test RMSE")

gg2 + coord_cartesian(ylim=c(20.0,20.25)) + ggtitle("Close-Up View, kfold 1 test error")
