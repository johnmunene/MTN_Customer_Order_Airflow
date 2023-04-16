# MTN_Customer_Order_Airflow
This program is meant to schedule task using dags in airflow. It consists of creating a dag and 3 main functions
The extract function gets the data from 3 sources and converts it into a dataframe
The transform function uses pandas to clean and perform other data analysis tasks
The load function uses the create engine function to load the transformed dat imto psotgres SQL
