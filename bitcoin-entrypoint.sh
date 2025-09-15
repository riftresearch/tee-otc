#!/bin/bash
set -euo pipefail
# /home/bitcoin/.bitcoin
bitcoin_data_dir="${BITCOIN_DATA_DIR}"
archive_url="https://rift-blockchain-archive.s3.us-west-1.amazonaws.com/bitcoind-datadir-2025-09-11.tar.lz4"

# Error out if required variables are empty
if [[ -z "$bitcoin_data_dir" ]]; then
    echo "Error: BITCOIN_DATA_DIR environment variable is empty or not set" >&2
    exit 1
fi

if [[ -z "$archive_url" ]]; then
    echo "Error: archive_url is empty" >&2
    exit 1
fi

echo "Checking Bitcoin data directory: $bitcoin_data_dir"

# Check if the data directory is empty (only contains wallet.dat or is completely empty)
# We consider it empty if there's no blocks/ directory or chainstate/ directory
if [[ ! -d "$bitcoin_data_dir/blocks" ]] && [[ ! -d "$bitcoin_data_dir/chainstate" ]]; then
    echo "Bitcoin data directory appears to be empty. Downloading blockchain data..."
    echo "This may take a while depending on your internet connection..."
    
    # Create a temporary directory for extraction
    temp_dir="$(mktemp -d)"
    trap "rm -rf $temp_dir" EXIT
    
    echo "Downloading from: $archive_url"
    
    # Download with aria2c for maximum speed with parallel connections
    archive_file="$temp_dir/blockchain-archive.tar.lz4"
    echo "Downloading to temporary file: $archive_file"
    echo "Using aria2c with 8 parallel connections for optimal speed..."
    
    # Use aria2c with optimal settings for large S3 downloads
    if aria2c \
        --max-connection-per-server=8 \
        --split=8 \
        --max-concurrent-downloads=1 \
        --continue=true \
        --retry-wait=5 \
        --max-tries=3 \
        --timeout=60 \
        --connect-timeout=10 \
        --file-allocation=none \
        --check-integrity=false \
        --summary-interval=10 \
        --download-result=hide \
        --console-log-level=info \
        --dir="$temp_dir" \
        --out="blockchain-archive.tar.lz4" \
        "$archive_url"; then
        
        echo "Download completed. Extracting archive..."
        if tar -I lz4 -xf "$archive_file" -C "$temp_dir"; then
            # Remove the archive file to save space
            rm -f "$archive_file"
            echo "Download and extraction successful"
            
            # Copy extracted data directly to bitcoin data directory
            echo "Copying blockchain data to $bitcoin_data_dir..."
            cp -r "$temp_dir/"* "$bitcoin_data_dir/"
            
            # Ensure proper ownership
            chown -R bitcoin:bitcoin "$bitcoin_data_dir"
            
            echo "Blockchain data initialization complete"
            echo "Data directory size: $(du -sh $bitcoin_data_dir | cut -f1)"
        else
            echo "Error: Failed to extract archive"
            rm -f "$archive_file"
            echo "Warning: Failed to download blockchain data. Bitcoin will sync from genesis block."
            echo "This will take significantly longer but is still functional."
        fi
    else
        echo "Warning: Failed to download blockchain data. Bitcoin will sync from genesis block."
        echo "This will take significantly longer but is still functional."
    fi
else
    echo "Bitcoin data directory already contains blockchain data"
    echo "Data directory size: $(du -sh $bitcoin_data_dir | cut -f1)"
fi

echo "Starting Bitcoin Core with arguments: $*"
exec bitcoind "$@"
