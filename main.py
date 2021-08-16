from utils import transform_events_data
from datetime import datetime
import glob

print('-------------- Date pipeline underway --------------')

def process_data():
    match = glob.glob(r'data/events_data*.json')
    return transform_events_data(match)

def output_data(data):
    now = datetime.now()
    output_csv = 'output/events_data_' + now.strftime("%d-%m-%Y_%H%M%S") + '.csv'
    data.to_csv(output_csv, index=False)

def main():
    data = process_data()
    output_data(data)

if __name__ == "__main__":
    main()

print('--------------  All files processed   --------------')
