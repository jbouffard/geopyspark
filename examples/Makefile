test:
	PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3
	spark-submit \
   --master 'local[*]' \
   --jars ../geopyspark/jar/geotrellis-backend-assembly-0.1.0.jar \
	 s3_tile_server.py
