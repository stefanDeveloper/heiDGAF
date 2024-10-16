#!/bin/bash

# Find all requirements*.txt files in the current directory
for req_file in $(ls requirements/requirements.*.txt); do
    echo "Installing from $req_file..."
    pip install -r "$req_file"
done

echo "All requirements installed!"
