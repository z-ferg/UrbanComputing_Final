import pandas as pd
from sklearn.neighbors import BallTree
import numpy as np


def find_nearest_transits(val_df: pd.DataFrame, sub_df: pd.DataFrame, bus_df: pd.DataFrame, debug=False):
    tra_df = combine_transit_df(sub_df=sub_df, bus_df=bus_df, debug=debug)
    ret = find_nearest_transit_for_properties(properties_df=val_df, transit_df=tra_df[tra_df["Transit"]], transit_type="sub")
    ret = find_nearest_transit_for_properties(properties_df=ret, transit_df=tra_df[~tra_df["Transit"]], transit_type="bus")
    return ret


def combine_transit_df(sub_df: pd.DataFrame, bus_df: pd.DataFrame, debug=False):
    # Initialize empty DataFrame with specified columns
    ret = pd.DataFrame(columns=["Name", "Longitude", "Latitude", "Transit"])

    # Append subway data
    if not sub_df.empty:
        sub_data = pd.DataFrame({
            "Name": sub_df["Station Name"] + " " + sub_df["Corner"],
            "Longitude": sub_df["Entrance Longitude"],
            "Latitude": sub_df["Entrance Latitude"],
            "Transit": True
        })
        ret = pd.concat([ret, sub_data], ignore_index=True)

    # Append bus data
    if not bus_df.empty:
        bus_data = pd.DataFrame({
            "Name": bus_df["On_Street"] + "/" + bus_df["Cross_Stre"] + " (" + bus_df["NTAName"] + ")",
            "Longitude": bus_df["Longitude"],
            "Latitude": bus_df["Latitude"],
            "Transit": False
        })
        ret = pd.concat([ret, bus_data], ignore_index=True)

    if debug:
        print(f"Total transit stops: {len(ret)}")
        print(f"Subway stops: {len(sub_df)}")
        print(f"Bus stops: {len(bus_df)}")

    return ret

# Claude Generated
def find_nearest_transit_for_properties(properties_df, transit_df, transit_type):
    # Convert coordinates to radians for haversine
    transit_coords = np.radians(transit_df[['Latitude', 'Longitude']].values)
    property_coords = np.radians(properties_df[['Latitude', 'Longitude']].values)
    
    # Build BallTree from transit stops
    tree = BallTree(transit_coords, metric='haversine')
    
    # Query for nearest transit stop for each property
    distances, indices = tree.query(property_coords, k=1)
    
    # Convert distances to km (distances are in radians)
    distances_km = distances.flatten() * 6371
    
    # Get the nearest transit stop names
    nearest_transit_names = transit_df.iloc[indices.flatten()]['Name'].values
    
    # Add results to properties dataframe
    properties_df = properties_df.copy()
    properties_df[f'{transit_type}_close'] = nearest_transit_names
    properties_df[f'{transit_type}_dist'] = distances_km
    
    return properties_df
