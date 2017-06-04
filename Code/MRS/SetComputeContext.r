if(file.exists("/dsvm"))
{
  # Set environment variables for the Data Science VM
  Sys.setenv(SPARK_HOME="/dsvm/tools/spark/current",
             YARN_CONF_DIR="/opt/hadoop/current/etc/hadoop", 
             JAVA_HOME = "/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.111-1.b15.el7_2.x86_64",
             PATH="/anaconda/envs/py35/bin:/dsvm/tools/cntk/cntk/bin:/usr/local/mpi/bin:/dsvm/tools/spark/current/bin:/anaconda/envs/py35/bin:/dsvm/tools/cntk/cntk/bin:/usr/local/mpi/bin:/dsvm/tools/spark/current/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/opt/hadoop/current/sbin:/opt/hadoop/current/bin:/home/remoteuser/.local/bin:/home/remoteuser/bin:/opt/hadoop/current/sbin:/opt/hadoop/current/bin"
  )
} else {
  Sys.setenv(SPARK_HOME="/usr/hdp/current/spark2-client")
}

useHDFS <- TRUE

if(useHDFS) {
  
  ################################################
  # Use Hadoop-compatible Distributed File System
  # N.B. Can be used with local or RxSpark compute contexts
  ################################################
  
  # ADDED FOR HDINSIGHT CLUSTER:
  rxOptions(hdfsHost = "hdfs://mycluster") # to access the cluster's locally attached HDFS storage
  
  rxOptions(fileSystem = RxHdfsFileSystem())
  
  #dataDir <- "/user/RevoShare/remoteuser/Data"
  dataDir <- "/user/sshuser"
  
  ################################################
  
  if(rxOptions()$hdfsHost == "default") {
    fullDataDir <- dataDir
  } else {
    fullDataDir <- paste0(rxOptions()$hdfsHost, dataDir)
  }  
} else {
  
  ################################################
  # Use Native, Local File System
  # N.B. Can only be used with local compute context
  ################################################
  
  rxOptions(fileSystem = RxNativeFileSystem())
  
  dataDir <- file.path(getwd(), "delayDataLarge")
  fullDataDir <- paste0("file://", dataDir)
  
  ################################################
}

################################################
# Distributed computing using Spark
################################################

startRxSpark <- function() {
  if (useHDFS) {
    # When running on an HDInsight cluster,
    # specifying numExecutors, executorCores,
    # and executorMem is optional
    rxSparkConnect(reset = T,
                   consoleOutput = TRUE, 
                   numExecutors = 4,  # FOR HDINSIGHT CLUSTER
                   executorCores = 8, # FOR HDINSIGHT CLUSTER
                   executorMem = "4g" # FOR HDINSIGHT CLUSTER
    )
  } else {
    cat("Using local compute context to process local data.\n")
  }
}

rxRoc <- function(...){
  previousContext <- rxSetComputeContext(RxLocalSeq())
  
  # rxRoc requires local compute context
  roc <- RevoScaleR::rxRoc(...)
  
  rxSetComputeContext(previousContext)
  
  return(roc)
}
