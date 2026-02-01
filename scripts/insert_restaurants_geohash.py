#!/usr/bin/env python3
"""Script to insert mock restaurants at varying distances around a center location"""
import sys
import os
import math
import random
import requests
import json

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

# API Configuration
API_URL = "https://htgicpllf2.execute-api.ap-south-1.amazonaws.com/default/api/v1"

# Center location (Nandyala, Andhra Pradesh)
CENTER_LAT = 15.4729283
CENTER_LNG = 78.4574246


def generate_coordinate_at_distance(center_lat, center_lng, distance_km, bearing_degrees):
    """
    Generate a coordinate at a specific distance and bearing from center point
    
    Args:
        center_lat: Center latitude
        center_lng: Center longitude
        distance_km: Distance in kilometers
        bearing_degrees: Bearing in degrees (0 = North, 90 = East, 180 = South, 270 = West)
        
    Returns:
        Tuple of (latitude, longitude)
    """
    R = 6371  # Earth's radius in km
    
    bearing = math.radians(bearing_degrees)
    lat1 = math.radians(center_lat)
    lon1 = math.radians(center_lng)
    
    lat2 = math.asin(
        math.sin(lat1) * math.cos(distance_km / R) +
        math.cos(lat1) * math.sin(distance_km / R) * math.cos(bearing)
    )
    
    lon2 = lon1 + math.atan2(
        math.sin(bearing) * math.sin(distance_km / R) * math.cos(lat1),
        math.cos(distance_km / R) - math.sin(lat1) * math.sin(lat2)
    )
    
    return math.degrees(lat2), math.degrees(lon2)


def create_restaurant(name, cuisine_list, lat, lng, rating, owner_id, image_url):
    """Create a restaurant via API"""
    try:
        print(f"\n📍 Creating restaurant: {name}")
        print(f"   Location: {lat:.6f}, {lng:.6f}")
        
        payload = {
            "locationId": "NANDYALA",
            "name": name,
            "latitude": lat,
            "longitude": lng,
            "isOpen": True,
            "cuisine": cuisine_list,
            "rating": rating,
            "ownerId": owner_id,
            "restaurantImage": image_url
        }
        
        response = requests.post(
            f"{API_URL}/restaurants",
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=10
        )
        
        if response.status_code == 201:
            data = response.json()
            restaurant_id = data.get('restaurantId')
            geohash = data.get('geohash', 'N/A')
            print(f"   ✅ Created: {restaurant_id} (geohash: {geohash})")
            return restaurant_id
        else:
            print(f"   ❌ Failed: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"   ❌ Error: {str(e)}")
        return None


def create_menu_items(restaurant_id, menu_items):
    """Create menu items for a restaurant"""
    print(f"   Adding {len(menu_items)} menu items...")
    
    for item in menu_items:
        try:
            response = requests.post(
                f"{API_URL}/restaurants/{restaurant_id}/menu",
                headers={"Content-Type": "application/json"},
                json=item,
                timeout=10
            )
            
            if response.status_code == 201:
                print(f"      ✓ {item['name']}")
            else:
                print(f"      ✗ {item['name']} - {response.status_code}")
        except Exception as e:
            print(f"      ✗ {item['name']} - {str(e)}")


# Restaurant templates with different cuisines and menu items
# NOTE: Menu items use 'restaurantPrice' (what restaurant charges)
# Backend auto-calculates customer price based on category markup:
#   Starters: +15% | Main Course: +20% | Rice & Biryani: +25%
#   Breads: +10% | Desserts: +30% | Beverages: +40%
RESTAURANT_TEMPLATES = [
    {
        "name": "Spice Garden",
        "cuisine": ["Indian", "North Indian", "Biryani"],
        "image": "https://images.unsplash.com/photo-1517248135467-4c7edcad34c4?w=400",
        "menu": [
            {"name": "Butter Chicken", "restaurantPrice": 300, "category": "Main Course", "isVeg": False, "description": "Creamy tomato curry"},
            {"name": "Paneer Tikka", "restaurantPrice": 250, "category": "Starters", "isVeg": True, "description": "Grilled cottage cheese"},
            {"name": "Chicken Biryani", "restaurantPrice": 350, "category": "Rice & Biryani", "isVeg": False, "description": "Fragrant rice"},
            {"name": "Dal Makhani", "restaurantPrice": 180, "category": "Main Course", "isVeg": True, "description": "Creamy lentils"},
            {"name": "Garlic Naan", "restaurantPrice": 70, "category": "Breads", "isVeg": True, "description": "Fresh baked bread"},
        ]
    },
    {
        "name": "Pizza Paradise",
        "cuisine": ["Italian", "Pizza", "Fast Food"],
        "image": "https://images.unsplash.com/photo-1555396273-367ea4eb4db5?w=400",
        "menu": [
            {"name": "Margherita Pizza", "restaurantPrice": 250, "category": "Main Course", "isVeg": True, "description": "Classic cheese pizza"},
            {"name": "Pepperoni Pizza", "restaurantPrice": 330, "category": "Main Course", "isVeg": False, "description": "Spicy pepperoni"},
            {"name": "Veg Supreme", "restaurantPrice": 290, "category": "Main Course", "isVeg": True, "description": "Loaded veggies"},
            {"name": "Garlic Bread", "restaurantPrice": 120, "category": "Starters", "isVeg": True, "description": "Crispy garlic bread"},
        ]
    },
    {
        "name": "Sushi Express",
        "cuisine": ["Japanese", "Sushi", "Asian"],
        "image": "https://images.unsplash.com/photo-1579584425555-c3ce17fd4351?w=400",
        "menu": [
            {"name": "Salmon Roll", "restaurantPrice": 380, "category": "Main Course", "isVeg": False, "description": "Fresh salmon"},
            {"name": "California Roll", "restaurantPrice": 320, "category": "Main Course", "isVeg": True, "description": "Avocado roll"},
            {"name": "Chicken Teriyaki", "restaurantPrice": 350, "category": "Main Course", "isVeg": False, "description": "Teriyaki bowl"},
            {"name": "Miso Soup", "restaurantPrice": 80, "category": "Starters", "isVeg": True, "description": "Japanese soup"},
        ]
    },
    {
        "name": "Burger Hub",
        "cuisine": ["American", "Burgers", "Fast Food"],
        "image": "https://images.unsplash.com/photo-1550547660-d9450f859349?w=400",
        "menu": [
            {"name": "Cheeseburger", "restaurantPrice": 160, "category": "Main Course", "isVeg": False, "description": "Classic burger"},
            {"name": "Veg Burger", "restaurantPrice": 120, "category": "Main Course", "isVeg": True, "description": "Veggie patty"},
            {"name": "French Fries", "restaurantPrice": 80, "category": "Starters", "isVeg": True, "description": "Crispy fries"},
            {"name": "Chocolate Shake", "restaurantPrice": 100, "category": "Beverages", "isVeg": True, "description": "Rich shake"},
        ]
    },
    {
        "name": "Thai Corner",
        "cuisine": ["Thai", "Asian", "Noodles"],
        "image": "https://images.unsplash.com/photo-1559314809-0d155014e29e?w=400",
        "menu": [
            {"name": "Pad Thai", "restaurantPrice": 320, "category": "Main Course", "isVeg": False, "description": "Thai noodles"},
            {"name": "Green Curry", "restaurantPrice": 350, "category": "Main Course", "isVeg": True, "description": "Spicy curry"},
            {"name": "Tom Yum Soup", "restaurantPrice": 270, "category": "Starters", "isVeg": False, "description": "Hot sour soup"},
            {"name": "Spring Rolls", "restaurantPrice": 180, "category": "Starters", "isVeg": True, "description": "Crispy rolls"},
        ]
    },
    {
        "name": "Dosa Point",
        "cuisine": ["South Indian", "Breakfast", "Vegetarian"],
        "image": "https://images.unsplash.com/photo-1589301760014-d929f3979dbc?w=400",
        "menu": [
            {"name": "Masala Dosa", "restaurantPrice": 65, "category": "Main Course", "isVeg": True, "description": "Crispy dosa"},
            {"name": "Idli Sambar", "restaurantPrice": 50, "category": "Main Course", "isVeg": True, "description": "Steamed idli"},
            {"name": "Vada", "restaurantPrice": 40, "category": "Starters", "isVeg": True, "description": "Fried lentil"},
            {"name": "Filter Coffee", "restaurantPrice": 25, "category": "Beverages", "isVeg": True, "description": "South Indian coffee"},
        ]
    },
]


def main():
    """Insert 25 restaurants at varying distances"""
    print("="*60)
    print("Inserting Mock Restaurants with Geohash")
    print(f"Center Location: {CENTER_LAT}, {CENTER_LNG}")
    print("="*60)
    
    # Generate 25 restaurants at different distances
    distances_km = [0.5, 0.8, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 
                     5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0]
    
    restaurants_created = 0
    
    for i, distance in enumerate(distances_km):
        if i >= len(RESTAURANT_TEMPLATES) * 4:  # Cycle through templates
            break
        
        # Select restaurant template (cycle through)
        template = RESTAURANT_TEMPLATES[i % len(RESTAURANT_TEMPLATES)]
        
        # Generate random bearing (direction)
        bearing = random.randint(0, 359)
        
        # Generate coordinate at distance
        lat, lng = generate_coordinate_at_distance(CENTER_LAT, CENTER_LNG, distance, bearing)
        
        # Add suffix to name for uniqueness
        restaurant_name = f"{template['name']} {i+1}"
        owner_id = f"OWNER{100 + i}"
        rating = round(random.uniform(3.8, 4.9), 1)
        
        # Create restaurant
        restaurant_id = create_restaurant(
            name=restaurant_name,
            cuisine_list=template['cuisine'],
            lat=lat,
            lng=lng,
            rating=rating,
            owner_id=owner_id,
            image_url=template['image']
        )
        
        if restaurant_id:
            # Add menu items
            menu_items = []
            for menu_item in template['menu']:
                menu_items.append({
                    **menu_item,
                    "isAvailable": True,
                    "image": template['image']
                })
            
            create_menu_items(restaurant_id, menu_items)
            restaurants_created += 1
            print(f"   ✅ Distance from center: {distance:.2f}km (bearing: {bearing}°)")
        
        # Small delay to avoid rate limiting
        import time
        time.sleep(0.5)
    
    print("\n" + "="*60)
    print(f"✅ Successfully created {restaurants_created} restaurants!")
    print("="*60)


if __name__ == "__main__":
    main()

