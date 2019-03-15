import csv
import json
import xml.etree.ElementTree as ET


#  write sql query output into a .csv file
def write_to_csv(query_output, file_name):
    with open(file_name + '.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', )
        for line in query_output:
            writer.writerow(line)
        csvfile.close()


# Gets a json format data and writes it to a .json file
def write_to_json(json_data, file_name):
    with open(file_name + '.json', 'w') as outfile:
        json.dump(json_data, outfile)
    outfile.close()


# Gets a dictionary with data and writes it to .xml file
def write_to_xml(content, file_name, header):
    root = ET.Element("root")
    doc = ET.SubElement(root, header)

    for key in content:
        ET.SubElement(doc, "field", name=key).text = str(content[key])

    tree = ET.ElementTree(root)
    tree.write("{}.xml".format(file_name))
