#!/usr/bin/env sh

# docker cp -L pipe.py master:/opt/app/pipe.py

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/pipe.py
