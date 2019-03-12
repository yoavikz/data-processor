
import csv


def write_to_csv(query_output, file_name):
    with open(file_name+'.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',)
        for line in query_output:
            writer.writerow(line)

