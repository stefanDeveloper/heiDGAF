#!/bin/bash

# Check if pip is available
if ! command -v pip &> /dev/null
then
    echo "pip could not be found, please install Python and pip first."
    exit
fi

# Find all requirements*.txt files in the current directory
for req_file in $(ls requirements/requirements.*.txt); do
    echo "Installing from $req_file..."
    pip install -r "$req_file"
done

echo "All requirements installed!"
