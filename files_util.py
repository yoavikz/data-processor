
import csv

#This is a util method to write sql query output into a csv file
def write_to_csv(query_output, file_name):
    with open(file_name+'.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',)
        for line in query_output:
            writer.writerow(line)

