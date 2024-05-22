run_pipeline:
	docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 /opt/app/pipe.py

format_files:
	black *.py
