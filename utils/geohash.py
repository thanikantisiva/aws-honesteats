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


def _decode_ranges(geohash: str) -> Tuple[Tuple[float, float], Tuple[float, float]]:
    """Decode geohash to its latitude/longitude bounds."""
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

    return (lat_range[0], lat_range[1]), (lon_range[0], lon_range[1])


def decode(geohash: str) -> Tuple[float, float]:
    """
    Decode geohash to latitude/longitude
    
    Args:
        geohash: Geohash string
        
    Returns:
        Tuple of (latitude, longitude)
    """
    lat_range, lon_range = _decode_ranges(geohash)
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
    lat_range, lon_range = _decode_ranges(geohash)
    lat = (lat_range[0] + lat_range[1]) / 2
    lon = (lon_range[0] + lon_range[1]) / 2
    precision = len(geohash)

    # Use actual decoded cell dimensions so neighboring cells are generated
    # correctly for coarse precisions like P4/P5 used in rider assignment.
    lat_offset = lat_range[1] - lat_range[0]
    lon_offset = lon_range[1] - lon_range[0]

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
        - 0.5km radius → Precision 6
        - 5km radius → Precision 5
        - 10km radius → Precision 5
        - 25km radius → Precision 4
    """
    # Choose a precision that slightly over-fetches candidate cells rather than
    # skipping nearby riders that sit in adjacent cells. This is especially
    # important for the 10km assignment radius, which should stay on P5.
    if radius_km <= 0.1:       # <= 100m
        return 7
    elif radius_km <= 1:       # <= 1km
        return 6
    elif radius_km <= 10:      # <= 10km
        return 5
    elif radius_km <= 50:      # <= 50km
        return 4
    elif radius_km <= 200:     # <= 200km
        return 3
    else:
        return 2

