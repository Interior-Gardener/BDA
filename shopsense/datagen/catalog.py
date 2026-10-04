"""Reference data used by the synthetic data generator: Indian cities,
product taxonomy, brands, names and marketing calendar."""

# city, state, region, tier, latitude, longitude, population weight
CITIES = [
    ("Mumbai", "Maharashtra", "West", 1, 19.08, 72.88, 10.0),
    ("Delhi", "Delhi", "North", 1, 28.61, 77.21, 10.0),
    ("Bengaluru", "Karnataka", "South", 1, 12.97, 77.59, 9.0),
    ("Hyderabad", "Telangana", "South", 1, 17.39, 78.49, 7.0),
    ("Chennai", "Tamil Nadu", "South", 1, 13.08, 80.27, 6.5),
    ("Kolkata", "West Bengal", "East", 1, 22.57, 88.36, 6.0),
    ("Pune", "Maharashtra", "West", 1, 18.52, 73.86, 6.0),
    ("Ahmedabad", "Gujarat", "West", 1, 23.02, 72.57, 4.5),
    ("Thane", "Maharashtra", "West", 2, 19.22, 72.98, 3.0),
    ("Gurugram", "Haryana", "North", 2, 28.46, 77.03, 3.0),
    ("Noida", "Uttar Pradesh", "North", 2, 28.54, 77.39, 2.8),
    ("Jaipur", "Rajasthan", "North", 2, 26.91, 75.79, 3.0),
    ("Lucknow", "Uttar Pradesh", "North", 2, 26.85, 80.95, 2.6),
    ("Surat", "Gujarat", "West", 2, 21.17, 72.83, 2.4),
    ("Chandigarh", "Chandigarh", "North", 2, 30.73, 76.78, 1.8),
    ("Indore", "Madhya Pradesh", "Central", 2, 22.72, 75.86, 2.0),
    ("Bhopal", "Madhya Pradesh", "Central", 2, 23.26, 77.41, 1.5),
    ("Nagpur", "Maharashtra", "Central", 2, 21.15, 79.09, 1.6),
    ("Kochi", "Kerala", "South", 2, 9.93, 76.27, 1.8),
    ("Coimbatore", "Tamil Nadu", "South", 2, 11.02, 76.96, 1.5),
    ("Visakhapatnam", "Andhra Pradesh", "South", 2, 17.69, 83.22, 1.4),
    ("Vadodara", "Gujarat", "West", 2, 22.31, 73.18, 1.3),
    ("Patna", "Bihar", "East", 3, 25.59, 85.14, 1.4),
    ("Bhubaneswar", "Odisha", "East", 3, 20.30, 85.82, 1.2),
    ("Guwahati", "Assam", "Northeast", 3, 26.14, 91.74, 1.1),
    ("Dehradun", "Uttarakhand", "North", 3, 30.32, 78.03, 0.9),
    ("Ranchi", "Jharkhand", "East", 3, 23.34, 85.31, 0.8),
    ("Mysuru", "Karnataka", "South", 3, 12.30, 76.64, 0.8),
]

# category -> (popularity weight, gross margin, return rate, subcategories)
# subcategory -> (min price, max price, item nouns)
CATEGORIES = {
    "Electronics": (1.25, 0.14, 0.05, {
        "Smartphones": (7999, 79999, ["Smartphone", "5G Phone", "Phone Pro", "Phone Lite"]),
        "Laptops": (29999, 149999, ["Laptop", "Ultrabook", "Gaming Laptop", "Notebook"]),
        "Headphones": (499, 14999, ["Wireless Earbuds", "Headphones", "Neckband", "ANC Headset"]),
        "Smartwatches": (1499, 29999, ["Smartwatch", "Fitness Band", "Smart Ring"]),
        "Mobile Accessories": (199, 2999, ["Phone Case", "Fast Charger", "Power Bank", "USB-C Cable", "Screen Guard"]),
    }),
    "Fashion": (1.35, 0.42, 0.14, {
        "Men's Clothing": (399, 3499, ["Cotton Shirt", "Slim Jeans", "Polo T-Shirt", "Kurta", "Chinos"]),
        "Women's Clothing": (449, 4999, ["Kurti", "Saree", "Maxi Dress", "Top", "Palazzo Set"]),
        "Footwear": (599, 6999, ["Running Shoes", "Sneakers", "Sandals", "Loafers", "Heels"]),
        "Bags & Wallets": (299, 3999, ["Backpack", "Handbag", "Leather Wallet", "Laptop Bag"]),
        "Watches": (799, 12999, ["Analog Watch", "Chronograph", "Digital Watch"]),
    }),
    "Home & Kitchen": (1.0, 0.30, 0.06, {
        "Cookware": (349, 4999, ["Non-Stick Pan", "Pressure Cooker", "Kadai", "Tawa", "Cookware Set"]),
        "Kitchen Appliances": (999, 14999, ["Mixer Grinder", "Air Fryer", "Induction Cooktop", "Electric Kettle"]),
        "Home Decor": (199, 3999, ["Wall Clock", "Table Lamp", "Photo Frame", "Indoor Planter"]),
        "Bedding": (499, 5999, ["Bedsheet Set", "Comforter", "Pillow Pair", "Blanket"]),
        "Storage": (199, 2499, ["Storage Box", "Shoe Rack", "Organiser"]),
    }),
    "Beauty & Personal Care": (0.95, 0.45, 0.04, {
        "Skincare": (149, 1999, ["Face Wash", "Moisturiser", "Sunscreen SPF50", "Vitamin C Serum"]),
        "Haircare": (149, 1499, ["Shampoo", "Hair Oil", "Conditioner", "Hair Serum"]),
        "Makeup": (199, 2499, ["Lipstick", "Kajal", "Foundation", "Compact"]),
        "Fragrances": (399, 4999, ["Eau de Parfum", "Body Mist", "Deodorant"]),
        "Grooming": (299, 3999, ["Beard Trimmer", "Hair Dryer", "Shaving Kit"]),
    }),
    "Books": (0.55, 0.22, 0.02, {
        "Fiction": (149, 699, ["Novel", "Thriller", "Mystery Novel"]),
        "Non-Fiction": (199, 899, ["Biography", "Self-Help Book", "History Book"]),
        "Academic": (299, 1499, ["Engineering Textbook", "Exam Guide", "Reference Book"]),
        "Children's Books": (99, 499, ["Picture Book", "Story Collection", "Activity Book"]),
    }),
    "Sports & Fitness": (0.6, 0.32, 0.06, {
        "Gym Equipment": (499, 9999, ["Dumbbell Set", "Resistance Bands", "Kettlebell", "Treadmill Mat"]),
        "Sportswear": (399, 2999, ["Track Pants", "Dry-Fit T-Shirt", "Sports Bra", "Shorts"]),
        "Yoga": (299, 2499, ["Yoga Mat", "Yoga Block", "Foam Roller"]),
        "Outdoor & Cycling": (599, 19999, ["Cricket Bat", "Football", "Cycle", "Badminton Racket"]),
    }),
    "Grocery & Gourmet": (0.85, 0.12, 0.01, {
        "Snacks": (49, 499, ["Namkeen Pack", "Dry Fruits Mix", "Protein Bar", "Cookies"]),
        "Beverages": (99, 999, ["Green Tea", "Filter Coffee", "Energy Drink", "Juice Pack"]),
        "Staples": (99, 1299, ["Basmati Rice", "Atta 10kg", "Cold-Pressed Oil", "Dal Combo"]),
        "Organic": (149, 999, ["Organic Honey", "Millet Mix", "Organic Jaggery"]),
    }),
    "Toys & Baby": (0.5, 0.35, 0.05, {
        "Toys": (199, 3999, ["Building Blocks", "RC Car", "Soft Toy", "Puzzle"]),
        "Baby Care": (199, 2999, ["Baby Diapers", "Baby Wipes", "Baby Lotion", "Feeding Bottle"]),
        "Board Games": (299, 1999, ["Board Game", "Card Game", "Chess Set"]),
    }),
}

# Complementary sub-categories (drives "frequently bought together").
COMPLEMENTS = {
    "Smartphones": ["Mobile Accessories", "Headphones"],
    "Laptops": ["Bags & Wallets", "Headphones", "Mobile Accessories"],
    "Smartwatches": ["Mobile Accessories"],
    "Kitchen Appliances": ["Cookware"],
    "Cookware": ["Kitchen Appliances", "Storage"],
    "Skincare": ["Makeup", "Haircare"],
    "Makeup": ["Skincare", "Fragrances"],
    "Men's Clothing": ["Footwear", "Watches"],
    "Women's Clothing": ["Footwear", "Bags & Wallets"],
    "Gym Equipment": ["Sportswear", "Yoga"],
    "Yoga": ["Sportswear"],
    "Baby Care": ["Toys"],
    "Snacks": ["Beverages"],
    "Staples": ["Snacks", "Organic"],
    "Fiction": ["Non-Fiction"],
    "Academic": ["Non-Fiction"],
}

BRAND_SYLLABLES_A = ["Volt", "Nexa", "Urban", "Aura", "Zen", "Tru", "Kira", "Mira", "Orbi",
                     "Vista", "Lumo", "Sava", "Indi", "Terra", "Nova", "Pixel", "Soma", "Riva"]
BRAND_SYLLABLES_B = ["ix", "tron", "craft", "wear", "leaf", "nest", "kart", "fit", "glow",
                     "loom", "ora", "byte", "kind", "hive", "wave", "mint"]
PRODUCT_TAGS = ["Pro", "Max", "Lite", "Plus", "Neo", "Prime", "Classic", "Air", "Ultra", "Eco"]

FIRST_NAMES_M = ["Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Krishna",
                 "Ishaan", "Rohan", "Kabir", "Aryan", "Dhruv", "Kartik", "Kushal", "Dhir",
                 "Rahul", "Amit", "Nikhil", "Siddharth", "Yash", "Harsh", "Pranav", "Om",
                 "Manish", "Varun", "Tanmay", "Parth", "Neel", "Aniket"]
FIRST_NAMES_F = ["Aadhya", "Ananya", "Diya", "Saanvi", "Pari", "Anika", "Navya", "Myra",
                 "Ira", "Riya", "Sneha", "Pooja", "Kavya", "Isha", "Meera", "Tanvi", "Nisha",
                 "Shreya", "Priya", "Aditi", "Khushi", "Sakshi", "Neha", "Janhvi", "Tara",
                 "Mahi", "Avni", "Kiara", "Rhea", "Siya"]
LAST_NAMES = ["Sharma", "Verma", "Patel", "Shah", "Mehta", "Iyer", "Nair", "Reddy", "Rao",
              "Gupta", "Singh", "Kumar", "Joshi", "Desai", "Kulkarni", "Thakar", "Soni",
              "Chatterjee", "Banerjee", "Das", "Pillai", "Menon", "Jain", "Agarwal",
              "Malhotra", "Kapoor", "Chopra", "Bhat", "Naik", "Pandey", "Mishra", "Yadav"]

ACQUISITION_CHANNELS = [("Organic Search", 0.28), ("Paid Search", 0.2), ("Social Media", 0.2),
                        ("Referral", 0.12), ("Email", 0.08), ("Direct", 0.12)]
TRAFFIC_SOURCES = ["Organic Search", "Paid Search", "Social Media", "Referral", "Email", "Direct"]
DEVICES = ["Mobile App", "Mobile Web", "Desktop"]
PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Cash on Delivery", "Net Banking", "Wallet", "EMI"]

# Named sale events (month, day, length in days, demand multiplier, typical discount)
SALE_EVENTS = [
    ("Republic Day Sale", 1, 20, 5, 1.55, 0.18),
    ("Holi Sale", 3, 10, 3, 1.25, 0.12),
    ("Summer Sale", 5, 15, 4, 1.25, 0.12),
    ("End of Season Sale", 6, 20, 7, 1.45, 0.25),
    ("Independence Day Sale", 8, 12, 5, 1.6, 0.2),
    ("Big Festive Sale", 10, 3, 8, 2.3, 0.3),
    ("Diwali Dhamaka", 10, 25, 7, 1.9, 0.25),
    ("Black Friday", 11, 25, 4, 1.4, 0.2),
    ("Year End Sale", 12, 24, 8, 1.35, 0.18),
]
