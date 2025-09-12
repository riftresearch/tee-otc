#!/bin/bash
set -euo pipefail

bitcoin_data_dir="/home/bitcoin/.bitcoin"
archive_url="https://rift-blockchain-archive.s3.us-west-1.amazonaws.com/bitcoind-datadir-2025-09-11.tar.lz4"

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
    if wget -O - "$archive_url" | tar -I lz4 -xf - -C "$temp_dir"; then
        echo "Download and extraction successful"
        
        # Copy extracted data directly to bitcoin data directory
        echo "Copying blockchain data to $bitcoin_data_dir..."
        cp -r "$temp_dir/"* "$bitcoin_data_dir/"
        
        # Ensure proper ownership
        chown -R bitcoin:bitcoin "$bitcoin_data_dir"
        
        echo "Blockchain data initialization complete"
        echo "Data directory size: $(du -sh $bitcoin_data_dir | cut -f1)"
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
