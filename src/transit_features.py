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

def get_nearest_transit(point, transit_tree, k=1, meters=False):
    """Given a latitude and longitude, return the distance to nearest transit stops.

    Args:
        point (tuple): (latitude, longitude) of the given point.
        transit_tree (BallTree): BallTree object containing transit stop locations.
        k (int, optional): Number of nearest neighbors to find. Defaults to 1.
        meters (bool, optional): If True, return distances in meters. Defaults to False (radians).
    
    Returns:
        list: distances to the k nearest transit stops (in meters if meters=True, else in radians).
        list: indices of the k nearest transit stops in the original dataset.
    """
    # Convert latitude and longitude to radians
    point_radians = np.deg2rad(point)
    
    # Get distances and indices of nearest subway entrances
    distances, indices = transit_tree.query(point_radians, k=k)
    
    if meters:
        return distances * 6371000, indices # Convert radians to meters
    else:
        return distances, indices


def transit_within_n(point, transit_tree, n_meters, dist=False, count=False, sort=False):
    """ Get the transit stops within n meters of a given latitude and longitude.

    Args:  
        point (tuple): (latitude, longitude) of the given point.
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
    point_radians = np.deg2rad(point)

    # Convert n_meters to radians
    radius_radians = n_meters / 6371000
    
    # Query the BallTree for points within specified radius
    ret_tuple = transit_tree.query_radius(
        point_radians, r=radius_radians, 
        count_only=count, return_distance=dist, sort_results=sort
    )
    
    if count:
        return ret_tuple[0]  # Return count of transit stops within n meters
    elif dist:
        return ret_tuple[0], ret_tuple[1] * 6371000  # Return indices and distances as meters
    else:
        return ret_tuple[0]  # Return indices of transit stops within n meters


# ==============================================================
#                   Subway Route Features
# ==============================================================
def get_subway_routes(point, transit_tree, n_meters, route_index):
    """ Get the subway rails available within n meters of a given latitude and longitude.

    Args:  
        point (tuple): (latitude, longitude) of the given point.
        transit_tree (BallTree): BallTree object containing transit stop locations.
        n_meters (int): Number of meters away from starting point to search within.
    
    Returns:

    """
    subway_ids = transit_within_n(
        point, transit_tree, n_meters, 
        dist=False, count=False, sort=False
    )
    
    routes = set()
    for sid in subway_ids:
        routes.update(route_index[sid].split(","))
    
    return len(subway_ids), ",".join(sorted(routes))


# ==============================================================
#                   Full Transit Pipeline
# ==============================================================
def transit_pipeline(ref_df, sub_df, bus_df, n_meters=500):
    """ Generate transit features in-place for ref_df:
            Number of buses within n meters         -> bus_in_{n}meters
            Number of subway stops within n meters  -> sub_in_{n}meters
            Subway routes within n meters           -> sub_routes_in_{n}meters
            Number of subway routes within n meters -> num_sub_routes_in_{n}meters
                ** {n} is just placeholder for parameter given

    Args:  
        ref_df (pandas.Dataframe) -> Reference dataframe to modify in place with transit features
        sub_df (pandas.Dataframe) -> Dataframe with subway stop locations
        bus_df (pandas.Dataframe) -> Dataframe with bus stop locations
        n_meters (int)            -> Optional, number of meters to check (defaults to 500)
    
    Returns:
        None -> Modify ref_df in place
    """
    sub_df.rename({'Entrance Longitude': 'Longitude', 'Entrance Latitude': 'Latitude'}, axis=1, inplace=True)

    bus_locs = bus_df[['Latitude', 'Longitude']]
    sub_locs = sub_df[['Latitude', 'Longitude']]
    
    bus_coords = np.deg2rad(bus_locs[['Latitude', 'Longitude']].to_numpy())
    sub_coords = np.deg2rad(sub_locs[['Latitude', 'Longitude']].to_numpy())
    
    bus_tree = BallTree(bus_coords, metric='haversine')
    sub_tree = BallTree(sub_coords, metric='haversine')
    
    ref_df[f'bus_in_{n_meters}m'] = ref_df.apply(
        lambda row: transit_within_n(
            np.array([[row['Latitude'], row['Longitude']]]), 
            transit_tree=bus_tree, 
            n_meters=n_meters, 
            dist=False, count=True
        ), axis=1
    )
    
    ref_df[[f'sub_in_{n_meters}m', f'sub_routes_in_{n_meters}m']] = ref_df.apply(
        lambda row: pd.Series(get_subway_routes(
            np.array([[row['Latitude'], row['Longitude']]]),
            transit_tree=sub_tree,
            n_meters=n_meters,
            route_index=sub_df['Routes']
        )),
        axis=1
    )
    
    ref_df[f'num_sub_routes_in_{n_meters}m'] = ref_df[f'sub_routes_in_{n_meters}m'].apply(
        lambda routes: len(routes.split(",")) if routes else 0
    )