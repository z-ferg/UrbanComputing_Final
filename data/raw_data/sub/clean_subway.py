import pandas as pd

# Load data
df = pd.read_csv('subway_raw.csv')

# Create the combined description
df['Entrance Description'] = (
    df['Station Name'].fillna('') + ' - ' +
    df['North South Street'].fillna('') + ' & ' +
    df['East West Street'].fillna('') + ' (' +
    df['Corner'].fillna('') + ')'
)

# Select relevant columns for the project
cleaned_df = df[['Entrance Description', 'Entrance Latitude', 'Entrance Longitude']].copy()

# Convert coords to float (handle any non-numeric) and drop invalid rows/coords
cleaned_df['Entrance Latitude'] = pd.to_numeric(cleaned_df['Entrance Latitude'], errors='coerce')
cleaned_df['Entrance Longitude'] = pd.to_numeric(cleaned_df['Entrance Longitude'], errors='coerce')
cleaned_df = cleaned_df.dropna(subset=['Entrance Latitude', 'Entrance Longitude'])

# Filter to NYC bounding box (found online) to remove outliers
cleaned_df = cleaned_df[
    (cleaned_df['Entrance Longitude'].between(-74.3, -73.7)) & 
    (cleaned_df['Entrance Latitude'].between(40.5, 40.9))
]

# Remove duplicates based on exact entrance coordinates
cleaned_df = cleaned_df.drop_duplicates(subset=['Entrance Latitude', 'Entrance Longitude'])

# Reset index for clean output
cleaned_df.reset_index(drop=True, inplace=True)

# Save and preveiw
cleaned_df.to_csv('cleaned_subway_entrances.csv', index=False)
print("Cleaned shape:", cleaned_df.shape)
print(cleaned_df.head())