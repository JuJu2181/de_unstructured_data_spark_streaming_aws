import random
import datetime

def generate_random_product(index):
    categories = {
        "Food": [
            "Parle-G Biscuit", "Dairy Milk Chocolate", "Lay's Chips", "Oreo Cookies", "Maggie Noodles", 
            "Kellogg's Corn Flakes", "KitKat", "Bournvita", "Tropicana Juice", "Pepsi"
        ],
        "Personal Care": [
            "Colgate Toothpaste", "Dove Shampoo", "Lifebuoy Soap", "Nivea Cream", "Vaseline Lotion", 
            "Garnier Face Wash", "Old Spice Deodorant", "Himalaya Face Pack", "Gillette Razor", "Head & Shoulders Shampoo"
        ],
        "Footwear": [
            "Nike Running Shoes", "Adidas Sneakers", "Puma Sandals", "Reebok Sports Shoes", "Skechers Loafers", 
            "Woodland Boots", "Bata Formal Shoes", "Crocs Slippers", "Fila Trainers", "New Balance Sneakers"
        ],
        "Electronics": [
            "Samsung Galaxy S21", "Apple iPhone 14", "Sony Headphones", "JBL Bluetooth Speaker", "Dell Laptop", 
            "HP Printer", "LG Smart TV", "Canon DSLR Camera", "Fitbit Smartwatch", "Logitech Keyboard"
        ]
    }
    
    category = random.choice(list(categories.keys()))
    product = random.choice(categories[category])
    product_code = f"Pr{1000 + index}"
    cost = random.randint(1000, 10000)  # Random cost between Rs.10 and Rs.500
    
    start_date = datetime.date.today() + datetime.timedelta(days=random.randint(0, 365))
    end_date = start_date + datetime.timedelta(days=365)
    
    content = f"""
{product_code}
Product: {product}

Category: {category}

Costs Rs.{cost}

Best From {start_date} to {end_date}
""".strip()
    
    return content

def save_to_file(content, filename):
    with open(filename, "w") as file:
        file.write(content)
    print(f"File '{filename}' has been created with random product details.")

if __name__ == "__main__":
    for i in range(1, 101):  # Generate 100 files with unique product codes
        product_details = generate_random_product(i)
        file_path = f"./source_files/txt_source/product_details_{i}.txt"
        save_to_file(product_details, file_path)
