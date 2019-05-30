# python-rabbitmq-sql
This project demonstrates how RabbitMQ, Python and SQLite are used together to deliver, receive and analyze data between 2 processes.

#Versions
Python version - 3.7.2
RabbitMQ version - 3.7.13
sqlite3

#External dependencies
pika 0.13.1 (pip3 install pika)

#General description
This project demonstrates a flow in which RabbitMQ is used to send data between one Python module (producer.py) to another (receiver.py). Once received, the data is used for db quering (sqlite3) and query results are stored in files (csv, json, xml) and written to new tables in the database. 
The db we manipulate is chinoook.db sample database, which stores  represents a digital media store, including tables for artists, albums, media tracks, invoices and customers.
The data we send between the processes is a Json format string which contains a path to db, a country name and a years

#Python modules
1. producer.py - Connects and sends a message to rabbitmq. Has a main method and used as a process from which we send the messages.
2. receiver.py - A process which collects data from a queue, and then execute logics (Storing in db, querying db, writing data to files etc.) on the data.
3. rabbitmq_connection.py - Used by producer and receiver modules to establish / close connection to rabbitmq. connects to localhost "queue" queue by default.
4. Utils modules - sql_util.py for connecting and querying the db, files_util.py to work with files in different formats.
  
#Files written
purchase_count_per_country.csv - containing the number of purchases for each country.
albums_purchased_per_country.json - containing a json representation of all albums names purchased for each country.
best_selling_album_in_*country*_since_*year*.xml - a file with the details of the best selling album in a certain country since a certain year #### Should be replaced with one general file updated with data for each query ####

#Database
By default we work with chinook.db sample database.
In the receiver.py logics we create 2 tables (tables are only created once and since then they are updated):

purchase_count_per_country - has 2 columns: country (Primary key), number_of_purchases
best_selling_album - 4 columns: album , number_of_sales, year(primary key), country(primary key)
   
   
#How to run
1. From command line (or IDE)  run receiver.py.

You should see the following message on your screen:
opened connection to rabbit :'<BlockingConnection impl=<SelectConnection OPEN socket=('::1', 57663, 0, 0)->('::1', 5672, 0, 0) params=<ConnectionParameters host=localhost port=5672 virtual_host=/ ssl=False>>>'
 [*] Waiting for messages. To exit press CTRL+C
  
2. From another terminal, run producer.py module to send messages to queue.
   
   You can supply message parameters in json format (from command line):
   python producer.py  "{'db': 'C:\\yourpath\\chinook.db', 'country': 'Brazil', 'year': 1998}" (for windows)
   python producer.py "{'db': '/yourpath/chinook.db', 'country': 'USA', 'year': 1999}" (for linux)
   
   You can also run producer.py without any arguments, then it will use a default message:
   "{'db': 'C:\\sqlite\\db\\chinook.db', 'country': 'USA', 'year': 1999}"  (for windows)
   
   Software limitation:
   Please note that the software is case-sensitive, so json message argument should be supplied just like in the examples (i.e, sending    'brazil' instead of 'Brazil' will throw an exception in the receiver.py). I will try to fix it soon.
   


