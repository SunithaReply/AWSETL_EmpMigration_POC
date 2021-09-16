# AWS_ETL_EmployeeMigration

Repository with Employee Migration data that uses EMR and Pyspark in AWS. The data has the information on employee details such as employeeID, firstname, lastname, email,address, unit, domain, yearsofexperience. It contains one million records and 9 columns in one csv as the main dataframe. The second csv contains the employee details who has changed the names. As a part of ETL process, the first steps are to generate/extract the data, transform and load them into the storage. It also check for the employee ID'S that has their name changed and then assign it to the respective unit and experience. 

**Objective**: To leverage ETL process in cloud. To evaluate and compare the ETL technologies in AWS Cloud i.e the Glue and EMR that also includes the cost optimization and computational effort. 

**TechStack**

1.AWS EMR

2.S3 - Data Storage

3.Pyspark

4.Python.
