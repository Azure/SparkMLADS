## download data
download.file("https://alizaidi.blob.core.windows.net/training/data/imdb-train.xdf",
              file.path("/home/", system("whoami", intern = TRUE), "SparkMLADS",
                        "imdb-train.xdf"))

download.file("https://alizaidi.blob.core.windows.net/training/data/imdb-test.xdf",
              destfile = file.path("/home/", system("whoami", intern = TRUE), "SparkMLADS",
                                   "imdb-test.xdf"))

## make local pointers
train_xdf <- RxXdfData("imdb-train.xdf")
test_xdf <- RxXdfData("imdb-test.xdf")

## make hdfs pointers
hdfs <- RxHdfsFileSystem()

rxHadoopMakeDir("/imdbdata/")

train_hdfs <- RxXdfData('/imdbdata/train/',
                        fileSystem = hdfs)
test_hdfs <- RxXdfData('/imdbdata/test/',
                       fileSystem = hdfs)

## import data to hdfs
rxDataStep(inData = train_xdf,
           outFile = train_hdfs)

rxDataStep(inData = test_xdf,
           outFile = test_hdfs)

# 
# # download mnist ----------------------------------------------------------
# 
# dir.create("Code/NLP/nn")
# download.file("https://alizaidi.blob.core.windows.net/training/nnet/LeCun5.nn",
#               "Code/NLP/nn/LeCun.nn")
# 
# download.file("https://alizaidi.blob.core.windows.net/training/nnet/MNIST.nn",
#               "Code/NLP/nn/MNIST.nn")
# 
# 
# 
# # Update CRAN and install pkgs --------------------------------------------


r <- getOption('repos')
r[["CRAN"]] <- paste0("https://mran.microsoft.com/snapshot/", Sys.Date() - 2)
options(repos = r)
install.packages(c('devtools', 'ggrepel'))
devtools::install_github("jbkunst/d3wordcloud")
