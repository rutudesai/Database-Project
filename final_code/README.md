# Rutgers Database Course Project
Build a Covid-19 Twitter data search application

1. Run command "docker-compose up -d" to start all the services.
2. Launch pgadmin on localhost:5050 port and execute the DDL and DML sql files to create the database, tables and stored procs.
3. Run either the Kafka data loader or the File based data loaders for each of the three data-stores.
4. Once data load is completed you can launch UI search or dashboard screen using command the "streamlit run <<ui_*.py>>"
5. Run command "docker-compose stop" to stop all services.