import os
import gzip
import shutil
from concurrent.futures import ThreadPoolExecutor

def decompress_single_file(args):
    gz_path, csv_path = args
    try:
        with gzip.open(gz_path, 'rb') as f_in:
            with open(csv_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(gz_path)
        return True, gz_path
    except Exception as e:
        return False, f"{gz_path} (Error: {e})"

def decompress_downloaded_files_parallel(root_dir="data/raw/openaq", max_workers=8):
    if not os.path.exists(root_dir):
        print(f"Directory {root_dir} does not exist.")
        return

    print(f"Scanning '{root_dir}' for compressed .gz files...")
    tasks = []
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".gz"):
                gz_path = os.path.join(root, file)
                csv_path = os.path.join(root, file[:-3])
                tasks.append((gz_path, csv_path))

    total_files = len(tasks)
    if total_files == 0:
        print("No compressed .gz files found.")
        return

    print(f"Found {total_files} files to decompress. Starting parallel processing with {max_workers} workers...")
    
    decompressed_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(decompress_single_file, tasks)
        
        for i, (success, path) in enumerate(results, start=1):
            if success:
                decompressed_count += 1
            else:
                failed_count += 1
                print(f"[FAILED] {path}")
            
            if i % 100 == 0 or i == total_files:
                print(f"Progress: {i}/{total_files} files processed... ({decompressed_count} successful, {failed_count} failed)")

    print(f"\nDecompression completed! {decompressed_count} files successfully extracted.")

# Run the optimized parallel decompression
decompress_downloaded_files_parallel()