# airflow_pipeline
A project using Apache Airflow

A prediction of the Covid-19 case numbers is calculated daily and the results are sent to the email of a set of users, as shown in the image below:

For the prediction of Covid-19 cases, we will download the daily bulletin of the number of cases maintained by the government of Brazil. At the end of each run, the data used to build the email is deleted so that the next run can happen without any problems
