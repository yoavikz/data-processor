# python-rabbitmq-sql
Using Python to write and read from RabbitMQ and execute relevant actions later

#Versions
Python version - 3.7.2
RabbitMQ version - 3.7.13
sqlit

#General description
This project demonstrates a flow in which RabbitMQ is used to send data between one Python module (producer.py) to aother (receiver.py). Once received, the data is used for db quering (sqlite3) and query results are stored in files (csv, json, xml) and written to new tables in the database. I used the chinoook.db sample database.

#How to run
1. From command line (or IDE)  run receiver.py.

You should see the following message on your screen:
opened connection to rabbit :'<BlockingConnection impl=<SelectConnection OPEN socket=('::1', 57663, 0, 0)->('::1', 5672, 0, 0) params=<ConnectionParameters host=localhost port=5672 virtual_host=/ ssl=False>>>'
 [*] Waiting for messages. To exit press CTRL+C
  
2. From another terminal, run producer.py module.
   
   You can supply message parameters in json format like this (from command line):
   python producer.py  "{'db': 'C:\\sqlite\\db\\chinook.db', 'country': 'Brazil', 'year': 1998}"
   
   You can also run producer.py without any arguments, then it will use a default message:
   '{'db': 'C:\\sqlite\\db\\chinook.db', 'country': 'USA', 'year': 1999}'
   
   Software limitation:
   Please note that the software is case-sensitive, so json message argument should be supplied just like in the examples (i.e, sending    'brazil' instead of 'Brazil' will throw an exception in the receiver.py logics). I will try to fix it soon.
   
#Python modules and dependency
  producer.py - connects and sends a message to rabbitmq. has a main method.
  receiver.py - connects to rabbitmq and receives messages, calls logics for each message (query, writing to db/file). has a main method.
  rabbitmq_connection.py - util used by producer.py and receiver.py to connect to db. connects to localhost "queue" queue by default
  files_util.py - util module to write query content to files. used by receiver.py
  sql_util.py - util module to work with sqlite db (query, create tables). user by receiver.py
  
#Files written
purchase_count_per_country.csv - containing the number of purchases for each country
albums_purchased_per_country.json - containing a list of all albums purchased for each country
best_selling_album_in_*country*_since_*year*.xml - a file with the details of the best selling album in a certain country since a certain year #### Should be replaced with one general file updated with data for each query ####

#Database
By default we work with chinook.db sample database.
In the receiver.py logics we create 2 tables (tables are only created once and since then they are updated):

purchase_count_per_country - has 2 columns: country (Primary key), number_of_purchases
best_selling_album - 4 columns: album , number_of_sales, year(primary key), country(primary key)
   
   
#Software
I used functional programming for this project for simplicity.
When the project will grow to a large scale project it might be necessary to transform it to an object oriented project.
It should be pretty simple because I tried to follow SOLID principles and split the responsibilities between the modules.
This repository is developed according to agile principles, with small pieces of code created in side branches before merging to maser.


