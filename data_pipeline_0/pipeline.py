from extract import extract
from transform import transform
from load import load

source_file_name = 'customer_order1'

extract_ins = extract(source_file_name = source_file_name)
extract_ins.extractData()

load_ins = load()
load_ins.loadCsv()
load_ins.loadDatabase()