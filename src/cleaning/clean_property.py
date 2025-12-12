import pandas as pd
import os
import glob
import io

#####################################

#         BEGIN CLEAN PROPERTY      #

######################################



# Config
input_dir = '../../data/raw_data/val/property_chunks/'  
output_file = '../../data/clean_data/val/cleaned_property_valuation.csv'
chunk_size = 100000
sample_fraction = 1.0  # 10% random sample to reduce size

# Clear output file if it exists
if os.path.exists(output_file):
    os.remove(output_file)

# Get list of all CSV files
csv_files = glob.glob(os.path.join(input_dir, '*.csv'))
print(f"Found {len(csv_files)} CSV files to process.")

# Process each file
total_rows_processed = 0
for file_idx, file_path in enumerate(csv_files, 1):
    print(f"Processing file {file_idx}/{len(csv_files)}: {os.path.basename(file_path)}")
    
    # Read in chunks
    for chunk_idx, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size)):
        # Select and rename columns
        if 'bble' in chunk.columns:  # Check if raw columns exist
            chunk_clean = chunk[['bble', 'boro', 'taxclass', 'fullval', 'latitude', 'longitude', 'nta', 'borough']].copy()
            chunk_clean.rename(columns={
                'bble': 'BBL', 'boro': 'BORO', 'taxclass': 'TAXCLASS', 'fullval': 'FULLVAL',
                'latitude': 'Latitude', 'longitude': 'Longitude', 'nta': 'NTA', 'borough': 'Borough'
            }, inplace=True)
        else:
            print(f"Warning: Expected columns not found in {file_path}. Skipping chunk.")
            continue
        
        # Convert types as needed
        chunk_clean['BORO'] = pd.to_numeric(chunk_clean['BORO'], errors='coerce').astype('Int64')
        chunk_clean['TAXCLASS'] = pd.to_numeric(chunk_clean['TAXCLASS'], errors='coerce').astype('Int64')
        chunk_clean['FULLVAL'] = pd.to_numeric(chunk_clean['FULLVAL'], errors='coerce').astype('Int64')
        chunk_clean['Latitude'] = pd.to_numeric(chunk_clean['Latitude'], errors='coerce')
        chunk_clean['Longitude'] = pd.to_numeric(chunk_clean['Longitude'], errors='coerce')
        chunk_clean['NTA'] = chunk_clean['NTA'].astype('category')
        chunk_clean['Borough'] = chunk_clean['Borough'].astype('category')
        
        # Filter: Drop missing/zero values, make sure were confined to NYC bounds
        chunk_clean = chunk_clean.dropna(subset=['FULLVAL', 'Latitude', 'Longitude'])
        chunk_clean = chunk_clean[chunk_clean['FULLVAL'] > 0]
        chunk_clean = chunk_clean[
            (chunk_clean['Longitude'].between(-74.3, -73.7)) & 
            (chunk_clean['Latitude'].between(40.5, 40.9))
        ]
        
        # Random sample to reduce size (maybe? idk we'll see)
        # chunk_clean = chunk_clean.sample(frac=sample_fraction, random_state=42).reset_index(drop=True)
        
        # Remove duplicates by BBL
        chunk_clean = chunk_clean.drop_duplicates(subset=['BBL'])
        
        # Append to output
        mode = 'a' if total_rows_processed > 0 else 'w'
        header = total_rows_processed == 0
        chunk_clean.to_csv(output_file, mode=mode, header=header, index=False)
        
        total_rows_processed += len(chunk_clean)
        print(f"  - Chunk {chunk_idx + 1}: Added {len(chunk_clean)} rows (total so far: {total_rows_processed})")

print(f"Final cleaned file: {output_file} with {total_rows_processed} rows.")

# Quick stats
final_df = pd.read_csv(output_file, nrows=1000000)  # Sample for stats
print("\nFULLVAL summary (from first 1M rows):")
print(final_df['FULLVAL'].describe())


##################################################################

#  SEPARATE FILES INTO SEPARATE PARTS FOR EASIER PROCESSING      #

##################################################################

# Configuration
input_file =    '../../data/clean_data/val/cleaned_property_valuation.csv'  
output_prefix = '../../data/clean_data/val/cleaned_property_part_' 
max_file_size_mb = 90 
chunk_size = 250000  

# Estimate bytes per row from a small sample
sample_df = pd.read_csv(input_file, nrows=100) 
estimated_row_size = sample_df.memory_usage(deep=True).sum() / len(sample_df)  # Bytes/row in memory
print(f"Estimated memory bytes per row: {estimated_row_size:.0f}")

# Calculate max bytes and split into files
max_file_size_bytes = max_file_size_mb * 1024 * 1024
current_file_num = 1
current_file_path = f'{output_prefix}{current_file_num}.csv'
current_file_size = 0
first_write = True

for chunk in pd.read_csv(input_file, chunksize=chunk_size, low_memory=False):
    # Estimate the CSV size accurately by generating the CSV string
    output = io.StringIO()
    chunk.to_csv(output, header=first_write, index=False)
    chunk_str = output.getvalue()
    csv_bytes = len(chunk_str.encode('utf-8'))
    output.close()
    
    # Check if adding this chunk would exceed the limit
    if current_file_size + csv_bytes > max_file_size_bytes:
        print(f"Starting new file {current_file_num} (final size: {current_file_size / (1024*1024):.1f} MB)")
        current_file_num += 1
        current_file_path = f'{output_prefix}{current_file_num}.csv'
        current_file_size = 0
        first_write = True
        # Reestimate for the new file, includes header
        output = io.StringIO()
        chunk.to_csv(output, header=first_write, index=False)
        chunk_str = output.getvalue()
        csv_bytes = len(chunk_str.encode('utf-8'))
        output.close()
    
    # Write the chunk to the file
    mode = 'w' if first_write else 'a'
    header = first_write
    chunk.to_csv(current_file_path, mode=mode, header=header, index=False)
    
    # Update the actual file size
    current_file_size = os.path.getsize(current_file_path)
    first_write = False
    
    print(f"Added to file {current_file_num}: {len(chunk)} rows (current size: {current_file_size / (1024*1024):.1f} MB)")

print(f"Created {current_file_num} files, each < {max_file_size_mb} MB.")
print("Files:", [f"{output_prefix}{i}.csv" for i in range(1, current_file_num + 1)])