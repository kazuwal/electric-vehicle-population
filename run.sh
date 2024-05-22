#!/usr/bin/env sh

docker cp -L pipe.py master:/opt/spark-app/pipe.py

docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark-app/pipe.py
