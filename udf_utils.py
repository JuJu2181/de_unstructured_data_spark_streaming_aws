from datetime import datetime
import re

def extract_p_id(file_content):
    file_content = file_content.strip()
    # First word of the file is the product id
    match = re.search(r'^(\w+)', file_content)
    p_id =  match.group(1) if match else None
    return p_id

def extract_p_name(file_content):
    # search for Product: <product_name>
    match = re.search(r'Product:\s*(.+)', file_content)
    p_name = match.group(1).strip() if match else None
    return p_name

def extract_p_category(file_content):
    # search for Category: <product_category>
    match = re.search(r'Category:\s*(.+)', file_content)
    p_category = match.group(1).strip() if match else None
    return p_category

def extract_p_price(file_content):
    # Search for Costs Rs.<price>
    match = re.search(r'Costs\s*Rs\.(\d+)', file_content)
    price = float(match.group(1)) if match else None
    return price

def extract_dates(file_content):
    match = re.search(r'Best From\s*(\d{4}-\d{2}-\d{2})\s*to\s*(\d{4}-\d{2}-\d{2})', file_content)
    if match:
        start_date = datetime.strptime(match.group(1), '%Y-%m-%d')
        end_date = datetime.strptime(match.group(2), '%Y-%m-%d')
        return start_date, end_date
    return None, None