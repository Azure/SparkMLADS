# Scalable Machine Learning with Spark and R on HDInsight

*Instructors*: Robert Horton, Mario Inchiosa, Ali Zaidi, Katherine Zhao

# Course Scripts: [https://aka.ms/mlads2017r](https://aka.ms/mlads2017r)

# Requirements

* An Azure subscription

# Tutorial Cluster Deployment Instructions

1.	Go to https://github.com/Azure/SparkMLADS/tree/master/azure-templates 
2.	Click the “Deploy to Azure” button
3.	Fill in the form and click “Purchase”. **IMPORTANT**: Set *Cluster Login User Name* = "**admin**" and *Ssh User Name* = "**sshuser**".

Make sure the resource group and cluster names start with a letter and contains only lowercase letters and numbers.

The password musts be at least 8 characters in length and must contain at least one digit, one non-alphanumeric character, one upper case letter and one lower case letter. Also, the password should not contain 3 consecutive letters from the username

Here is an example:

    ![Image of creating a new cluster](https://raw.githubusercontent.com/Azure/SparkMLADS/master/imgs/portal-template.PNG)

4.	Wait 30-40 minutes for the cluster to deploy

5.	We will run our R scripts using the RStudio IDE. To launch RStudio in your browser, from the cluster overview in the Azure portal, click "R Server dashboards" and then "R Studio server". At the first login screen, enter "admin" and the password you supplied. At the second login screen, enter "sshuser" and the password you supplied.

    ![Image of the cluster overview](https://raw.githubusercontent.com/Azure/SparkMLADS/master/imgs/cluster-overview.PNG)

6.	Once in RStudio, go to the Files pane in the lower right-hand corner and click on "SparkMLADS" and then "Code". Here you will find the directories for the hands-on tutorial scripts.

# Contributing

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
