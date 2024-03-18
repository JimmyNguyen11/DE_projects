import pandas as pd 
import glob 
import xml.etree.ElementTree as ET 
from datetime import datetime 
log_file = 'log_file.txt' 
target = 'transformed_data.csv' 
def extract_from_csv(file): 
    a = pd.read_csv(file) 
    return a 
def extract_from_json(file): 
    b = pd.read_json(file, lines=True) 
    return b 
def extract_from_xml(file): 
    c = pd.DataFrame(columns = ['car_model', 'year_of_manufacture', 'price', 'fuel']) 
    tree = ET.parse(file) 
    root = tree.getroot() 
    for car in root: 
        car_model = car.find('car_model').text 
        year_of_manufacture = int(car.find('year_of_manufacture').text) 
        price = float(car.find('price').text) 
        fuel = car.find('fuel').text 
        c = pd.concat([c, pd.DataFrame([{'car_model':car_model, 'year_of_manufacture':year_of_manufacture, 'price':price, 'fuel':fuel}])], ignore_index=True) 
    return c 
def extract(): 
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel']) 
    # process all csv files 
    for csvfile in glob.glob("*.csv"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True) 
    # process all json files 
    for jsonfile in glob.glob("*.json"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True) 
    # process all xml files 
    for xmlfile in glob.glob("*.xml"): 
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True) 
    return extracted_data 
    
def transform(data): 
    data['price'] = round(data.price, 2)
    return data

def load(target, transformed_data):
    transformed_data.to_csv(target)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n') 

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load(target,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 