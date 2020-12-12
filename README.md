# Space Flight Search Engine - CougSpark

![space_fligh_search](/static/enriched_diagram_3d_2.png)

## 1. introduction
This project implements a flight search engine using a graph data structure by datasets provided from openflights.org.

## 2. Usage
We use Docker containers to configure our development and deployment environment.

For testing under the deployment environment, follow below steps.
1. Create a shell scirpt with your repository location
    ```sh
    start-deployment.sh

    #!/bin/bash

    echo "$1" > <your_repo_loc>/conf_clustering

    docker run -it --rm --net host \
            --name deployment \
            -v <your_repo_loc>:/root/work:rw \
            ajkimdev/cougspark-deploy:latest
    ```

2. Pull docker image and execute the shell script with ip address
    ```sh
    docker pull ajkimdev/cougspark-deploy:latest

    chmod +x start-deployment.sh

    ./start-deployment.sh <your host ip address> # i.e. AWS EC2 public DNS (IPv4)
    ```
3. In container, executes Spark cluster
   ```sh
   # configure master and slave for spark
   start-clustering.sh
   ```

4. Now you can see Spark master Web UI in ```<your host ip address>:8080```
5. Edit ParGraphAgg.py with Spark master ip in line 12
   ```python
   self.spark = SparkSession.builder.appName(
       "space-flight-search").master('spark://<master ip>:7077').getOrCreate()
   ```

6. Here are two options to run the application
   1. run web server to see front end web page to test 
      ```sh
      # run flask web server with spark-submit command
      start-server.sh
      ```
   2. run your parallel algorithm python file on Spark
      ```sh
      ./submit.sh <file name>.py
      ```

