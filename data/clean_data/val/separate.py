import pandas as pd
import os
import io

# Configuration
input_file = 'cleaned_property_valuation.csv'  
output_prefix = 'cleaned_property_part_' 
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