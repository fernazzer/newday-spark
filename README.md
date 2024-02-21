## Running Spark App

Example to run:

/bin/spark-submit /../new-day/newday_spark/app.py

Ensure to provide full file location of app.py

Example on my computer:
/opt/homebrew/bin/spark-submit /Users/lukefernando/Workspace/Python/new-day/newday_spark/app.py

Results are saved to "outputs" folder - Spark has not been repartitioned so expect multiple part files for some data frames