import importlib
import gc
import math

import numpy as np
import pandas as pd
import data_cleaning as dc

from sklearn.neighbors import BallTree

# ==============================================================
#                   Nearest Transit Features
# ==============================================================

def get_nearest_transit(latitude, longitude, transit_tree, k=1, meters=False):
    """Given a latitude and longitude, return the distance to nearest transit stops.

    Args:
        latitude (float): Latitude of the given point.
        longitude (float): Longitude of the given point.
        transit_tree (BallTree): BallTree object containing transit stop locations.
        k (int, optional): Number of nearest neighbors to find. Defaults to 1.
        meters (bool, optional): If True, return distances in meters. Defaults to False (radians).
    
    Returns:
        list: distances to the k nearest transit stops (in meters if meters=True, else in radians).
        list: indices of the k nearest transit stops in the original dataset.
    """
    # Convert latitude and longitude to radians
    point_radians = np.deg2rad([[latitude, longitude]])
    
    # Get distances and indices of nearest subway entrances
    distances, indices = transit_tree.query(point_radians, k=k)
    
    if meters:
        return distances * 6371000, indices # Convert radians to meters
    else:
        return distances, indices


def transit_within_n(latitude, longitude, transit_tree, n_meters, dist=False, count=False, sort=False):
    """ Get the transit stops within n meters of a given latitude and longitude.

    Args:
        latitude (float): Latitude of the given point.
        longitude (float): Longitude of the given point.
        transit_tree (BallTree): BallTree object containing transit stop locations.
        n_meters (int): Number of meters away from starting point to search within.
        dist (bool, optional): Query arg for getting distance scores. Defaults to False.
        count (bool, optional): Query arg for returning counts only. Defaults to False.
        sort (bool, optional): Query arg for returning sorted distances. Defaults to False.
    
    Returns:
        int:  if count=True -> int count of transit stops within n meters if count=True
        list: if count=False -> list of transit IDs within n meters if count=False
        list: if dist=True -> list of distances to transit stops within n meters, sorted if sort=True
    """
    # Convert latitude and longitude to radians
    point_radians = np.deg2rad([[latitude, longitude]])

    # Convert n_meters to radians
    radius_radians = n_meters / 6371000
    
    # Query the BallTree for points within specified radius
    return transit_tree.query_radius(
        point_radians, r=radius_radians, 
        count_only=count, return_distance=dist, sort_results=sort
    )


# ==============================================================
#                   Subway Route Features
# ==============================================================
def get_subway_routes(...):
    pass