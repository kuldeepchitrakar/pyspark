Pyspark project structure for production

It has been tested for spark on windows

to run:

1. Zip the jobs folder
2. open command line and traverse to pyspark folder
3. spark-submit --py-files jobs.zip src/main.py --job wordcount --res-path "c:\pyspark\src\jobs"

