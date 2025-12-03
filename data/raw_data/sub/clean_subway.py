import pandas as pd

# Load the full raw CSV and select relevant columns for the project
df = pd.read_csv('subway_raw.csv')
cleaned_df = df[['Station Name', 'Entrance Latitude', 'Entrance Longitude']].copy()

# Convert coords to float
cleaned_df['Entrance Latitude'] = pd.to_numeric(cleaned_df['Entrance Latitude'], errors='coerce')
cleaned_df['Entrance Longitude'] = pd.to_numeric(cleaned_df['Entrance Longitude'], errors='coerce')

# Drop rows with missing or invalid coords
cleaned_df = cleaned_df.dropna(subset=['Entrance Latitude', 'Entrance Longitude'])

# Filter to NYC bounding box to remove outliers
cleaned_df = cleaned_df[
    (cleaned_df['Entrance Longitude'].between(-74.3, -73.7)) & 
    (cleaned_df['Entrance Latitude'].between(40.5, 40.9))
]

# Remove duplicates based on exact entrance coordinates and reset index for clean output
cleaned_df = cleaned_df.drop_duplicates(subset=['Entrance Latitude', 'Entrance Longitude'])
cleaned_df.reset_index(drop=True, inplace=True)

# Save and preview
cleaned_df.to_csv('cleaned_subway_entrances.csv', index=False)
print("Cleaned shape:", cleaned_df.shape)
print(cleaned_df.head())