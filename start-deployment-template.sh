#!/bin/bash

echo "$1" > <your_repo_loc>/conf_clustering

docker run -it --rm --net host \
        --name deployment \
        -v <your_repo_loc>:/root/work:rw \
        ajkimdev/cougspark-deploy:latest