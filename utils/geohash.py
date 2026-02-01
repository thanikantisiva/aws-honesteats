"""Geohash utility functions for spatial indexing"""
import math
from typing import List, Tuple


# Base32 encoding for geohash
BASE32 = "0123456789bcdefghjkmnpqrstuvwxyz"


def encode(latitude: float, longitude: float, precision: int = 7) -> str:
    """
    Encode latitude/longitude to geohash
    
    Args:
        latitude: Latitude (-90 to 90)
        longitude: Longitude (-180 to 180)
        precision: Length of geohash (default 7, ~153m resolution)
        
    Returns:
        Geohash string
    """
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    geohash = []
    bits = 0
    bit = 0
    even = True
    
    while len(geohash) < precision:
        if even:
            # longitude
            mid = (lon_range[0] + lon_range[1]) / 2
            if longitude > mid:
                bit |= (1 << (4 - bits))
                lon_range[0] = mid
            else:
                lon_range[1] = mid
        else:
            # latitude
            mid = (lat_range[0] + lat_range[1]) / 2
            if latitude > mid:
                bit |= (1 << (4 - bits))
                lat_range[0] = mid
            else:
                lat_range[1] = mid
        
        even = not even
        bits += 1
        
        if bits == 5:
            geohash.append(BASE32[bit])
            bits = 0
            bit = 0
    
    return ''.join(geohash)


def decode(geohash: str) -> Tuple[float, float]:
    """
    Decode geohash to latitude/longitude
    
    Args:
        geohash: Geohash string
        
    Returns:
        Tuple of (latitude, longitude)
    """
    lat_range = [-90.0, 90.0]
    lon_range = [-180.0, 180.0]
    even = True
    
    for char in geohash:
        idx = BASE32.index(char)
        
        for i in range(4, -1, -1):
            bit = (idx >> i) & 1
            
            if even:
                # longitude
                mid = (lon_range[0] + lon_range[1]) / 2
                if bit == 1:
                    lon_range[0] = mid
                else:
                    lon_range[1] = mid
            else:
                # latitude
                mid = (lat_range[0] + lat_range[1]) / 2
                if bit == 1:
                    lat_range[0] = mid
                else:
                    lat_range[1] = mid
            
            even = not even
    
    latitude = (lat_range[0] + lat_range[1]) / 2
    longitude = (lon_range[0] + lon_range[1]) / 2
    
    return latitude, longitude


def get_neighbors(geohash: str) -> List[str]:
    """
    Get all 8 neighboring geohashes
    
    Args:
        geohash: Center geohash
        
    Returns:
        List of 8 neighboring geohashes (N, NE, E, SE, S, SW, W, NW)
    """
    lat, lon = decode(geohash)
    precision = len(geohash)
    
    # Approximate degree offsets based on geohash precision
    # Precision 7 ≈ 153m x 153m cell
    lat_offset = 0.0014  # ~153m at equator
    lon_offset = 0.0014
    
    neighbors = []
    
    # 8 directions: N, NE, E, SE, S, SW, W, NW
    offsets = [
        (lat_offset, 0),           # N
        (lat_offset, lon_offset),  # NE
        (0, lon_offset),           # E
        (-lat_offset, lon_offset), # SE
        (-lat_offset, 0),          # S
        (-lat_offset, -lon_offset),# SW
        (0, -lon_offset),          # W
        (lat_offset, -lon_offset)  # NW
    ]
    
    for lat_off, lon_off in offsets:
        neighbor_geohash = encode(lat + lat_off, lon + lon_off, precision)
        if neighbor_geohash not in neighbors and neighbor_geohash != geohash:
            neighbors.append(neighbor_geohash)
    
    return neighbors


def get_precision_for_radius(radius_km: float) -> int:
    """
    Get appropriate geohash precision for given radius
    
    Goal: Choose precision where cell size is roughly equal to the search radius
    This allows querying center + 8 neighbors to cover the circular search area
    
    Args:
        radius_km: Radius in kilometers
        
    Returns:
        Geohash precision level (1-9)
    
    Examples:
        - 0.5km radius → Precision 6 (~0.61km cells)
        - 2km radius → Precision 5 (~2.4km cells)
        - 5km radius → Precision 5 (~2.4km cells)
        - 15km radius → Precision 4 (~20km cells)
    """
    # Geohash precision to approximate cell size mapping
    # precision: (width x height in km at equator)
    # For search: Use precision where cell_size is close to radius
    
    if radius_km <= 0.05:      # < 50m
        return 7               # ~76m cells (P7)
    elif radius_km <= 0.5:     # < 500m
        return 6               # ~610m cells (P6)
    elif radius_km <= 5:       # < 5km
        return 5               # ~2.4km cells (P5)
    elif radius_km <= 15:      # < 15km
        return 4               # ~20km cells (P4)
    elif radius_km <= 50:      # < 50km
        return 3               # ~78km cells (P3)
    else:
        return 2               # ~625km cells (P2)

