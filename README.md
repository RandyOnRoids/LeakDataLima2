# Life-Bear Data Processing Code
![AICleanFinal](AICleanFinal.png)

## Overview
This repository contains a Python script designed to process a large CSV dataset related to Protexxa/Life-Bear. It includes functionality for splitting large files, handling missing and invalid data, removing empty lines, and structuring the data for further analysis. The script operates on CSV files stored in Google Drive and processes them in chunks to avoid memory overload.

This is our first attempt at in-depth data cleaning.  Much of this code was inspired by other more experienced coders and we made a few changes to suit the data we were given to clean.  We will constantly be making changes as we learn and develop and therefore, there will be a change log added below as changes are made!

Please see below our current example output.  Some of the data is still missing or needs further cleaning. However, as stated above, changes will be made over time. 

**Chad & Victor Team Lima**

## Prerequisites
Before running the script, ensure that you have the following:
Google Colab or a local Python environment with access to Google Drive.
Required Python libraries:
pandas
numpy
os
csv
re
random
datetime
dateutil

## Setup Instructions

### Step 1: Mount Google Drive
Ensure you have access to the dataset in your Google Drive. The following command mounts Google Drive for access in Google Colab:

    from google.colab import drive
    drive.mount('/content/drive')

### Step 2: Define File Paths
Set the path to your CSV file and output folder in Google Drive:

    file_path = '/content/drive/My Drive/Protexxa/Life-Bear/Life-Bear-Data.csv'
    output_folder = '/content/drive/My Drive/Protexxa/Life-Bear/output'
 
## Functions
### 1. Split Large Files into Chunks
This function splits large CSV files into smaller chunks based on a specified size (in MB).

    def split_file(file_path, chunk_size, output_directory):
        # Splits large files into smaller chunks to avoid memory issues.

Usage example:

    split_file(file_path, 100, output_folder)
This will split the file into 100 MB chunks and save them to the output folder.

### 2. Handle Missing Data and Remove Duplicates
This part of the script checks for missing values and removes duplicates based on key columns (login_id, mail_address, and salt).

    condo_dataset = pd.read_csv(file_path, low_memory=True, sep=';')
    condo_wd_dataset = condo_dataset.drop_duplicates(subset=["login_id", "mail_address", "salt"])
    condo_wd_dataset.to_csv(output_file_name, encoding='utf-8', index=False)
    
### 3. Validate and Process Data
This function processes the input CSV, validates data (like email and login IDs), and structures the output. It also handles invalid data by sending it to a separate "garbage" file.

    def process_file(input_file_path, output_folder):
        # Processes and validates CSV data, writing valid data to the output file and invalid data to the garbage file.
Example usage:

    process_file(input_file_path, output_folder)

### 4. Remove Empty Lines and Unwanted Text
The script removes empty lines and lines with unwanted placeholders (e.g., ,,,, and NULL,,,NULL,) from the processed CSV files.

    def remove_empty_lines(input_file, output_file):
        # Removes empty lines and specific unwanted text patterns from the CSV files.
Usage example:

    remove_empty_lines(input_file_path, output_file_path)

## File Structure
This is an example of how our file structure was setup so that everything was organized. 

    /content/drive/My Drive/Protexxa/Life-Bear/
        ├── Life-Bear-Data.csv              # Original data
        ├── output/                         # Directory for chunked or processed files
        │   ├── lifebear_001.csv            # Example of chunked file
        │   ├── processed/                  # Directory for processed files
        │   │   ├── lifebear_structured.csv # Example of structured data
        │   │   ├── garbage/                # Invalid records
        │   └── removed_empty_lines/        # Directory for cleaned files

## Example Workflow
Split Files: Split large datasets into smaller chunks.
Process Data: Validate the data and output structured CSV files.
Handle Missing Data: Remove duplicates and clean the dataset.
Remove Empty Lines: Remove unwanted text and clean the CSV files.
Notes
Ensure that your file paths are correct and accessible via Google Drive.
Modify the delimiter used in the pd.read_csv() function if your CSV uses a non-standard delimiter (e.g., ;).
License
This project is licensed under the MIT License.

