# RiskAI_Test
This is a work sample which takes Big data into prospect and works with almost 8000 csv files of stocks and etfs data to create a batch process and create a docker file(Docker is a software platform that allows you to build, test, and deploy applications quickly).
This project uses spark streaming services.

To run the docker file. You will have to download all the data for stock and efts from https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset and save it in a new directory as the docker files inside a folder named data.

    //If the data is available in a remote directory directly accessible through docker, our docker could be independent of 
    //copying any data and just running the dockerfile independently:
    //# Create a directory for the data
    //RUN mkdir /data

    //# Download necessary files from a remote directory
    //RUN curl -O <remote_file_url> -o /data/<filename>
    //Since the files are in a Kaggle repository in .zip format, we will be copying them manually to our docker repository in folder named 'data'
    //Now, to put data to HDFS(Hadoop distributed file storage) in Spark(Spark leverages in-memory processing and can integrate with Hadoop. Therefore, faster) using Scala(Language).

Your directiories should be like: your-docker-repository-in-system/data/etfs/and stocks

Your dockerfile should be here: your-docker-repository-in-system/Dockerfile

The Dockerfile is uploaded in the github repository.

After the docker is set up and the scala environment with Spark is up and running you need to copy the code from Problem Set 1 to Problem Set 3 in the scala environment. Preferrably copy each problem seperately as problem 2 and 3 takes up some time depending on your hardware.

Further Key Points are provided in the Work-Sample.docx file in this repository.
