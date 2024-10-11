from google.colab import drive # Finalized Code ||
drive.mount('/content/drive')

import pandas as pd
import numpy as np
import os # Import the os module for file operations
import csv
import re
import random
from datetime import datetime
from dateutil import parser

# Replace 'Your Folder Path' with the actual path to your CSV file in Google Drive
file_path = '/content/drive/My Drive/Protexxa/Life-Bear/Life-Bear-Data.csv' # Assign the file path to a variable
df = pd.read_csv(file_path)

print(df.head()) # Display the first few rows of the dataframe

def split_file(file_path, chunk_size, output_directory):
    os.makedirs(output_directory, exist_ok=True)

    with open(file_path, 'rb') as file:
        file_count = 0
        current_size = 0
        output_file = None

        for line in file:
            if output_file is None or current_size >= chunk_size * 1024 * 1024:
                if output_file:
                    output_file.close()
                file_count += 1
                current_size = 0
                file_name = f'lifebear_{str(file_count).zfill(3)}.csv'
                current_file_path = os.path.join(output_directory, file_name)
                output_file = open(current_file_path, 'w', encoding='utf-8')

            try:
                decoded_line = line.decode('utf-8')  # Try decoding with UTF-8
            except UnicodeDecodeError:
                decoded_line = line.decode('latin-1')  # If that fails, try decoding with Latin-1

            output_file.write(decoded_line)
            current_size += len(line)

        if output_file:
            output_file.close()

    print(f'Splitting complete. Total files created: {file_count}')

# Usage
output_folder = '/content/drive/My Drive/Protexxa/Life-Bear/output'  # Define output_folder
split_file(file_path, 100, output_folder)  # Call split_file with the defined file_path


# Step 1: Handling Missing Data

# Check for missing values
print("Missing values per column:")
print(df.isnull().sum())

# Drop all duplicate records based on login_id, mail_address & salt/hash
condo_dataset = pd.read_csv(file_path, low_memory=True, sep=';') # Added sep=';' to specify the delimiter

# Print the actual column names in your DataFrame
print(condo_dataset.columns)

# Adjust the subset based on the actual column names
# For example, if your columns are named 'Login_ID', 'Mail_Address', and 'Salt', use:
condo_wd_dataset = condo_dataset.drop_duplicates(subset=["login_id", "mail_address", "salt"])
# or if the delimiter is not comma use sep=';' if the delimiter is a semicolon


# Assign output_file_name
output_file_name = '/content/drive/My Drive/Protexxa/Life-Bear/output/condo_wd_dataset.csv'
condo_wd_dataset.to_csv(output_file_name, encoding='utf-8', index=False)

condo_dataset = pd.read_csv(output_file_name, low_memory=True)
# condo_dataset.head(5)
condo_dataset.info()

def to_camel_case(s):
    if s is None:
        return None
    return ' '.join(word.capitalize() for word in s.split())

def to_lower(s):
    if s is None:
        return None
    return s.lower()

def convert_date_format(date_str):
    try:
        # Parse the input date string in various formats
        input_date = datetime.strptime(date_str, "%Y-%m-%d")
        # Convert the date to the desired format
        output_date_str = input_date.strftime("%m/%d/%Y")
        return output_date_str
    except ValueError:
        # If there's a ValueError, return None or leave it unmodified
        return None

def validate_login_id(login_id):
    name_pattern = re.compile(r'^[a-zA-Z0-9&|#()/\-._ ]*$')
    return bool(re.fullmatch(name_pattern, str(login_id)))

def validate_mail_address(mail_address):
    email_pattern = re.compile(r'^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w+$')
    return bool(re.fullmatch(email_pattern, str(mail_address)))

def only_letters(text):
    return text.isalpha()

def only_numbers(text):
    return text.isdigit()

def process_file(input_file_path, output_folder):
    filename = os.path.splitext(os.path.basename(input_file_path))[0]
    output_file_path = os.path.join(output_folder, f"{filename}_structured.csv")

    # Create garbage subfolder if it doesn't exist
    garbage_folder = os.path.join(output_folder, "garbage")
    os.makedirs(garbage_folder, exist_ok=True)
    garbage_file_path = os.path.join(garbage_folder, f"{filename}_garbage.csv")

    with open(output_file_path, 'w', newline='', encoding='utf8') as output_file, open(garbage_file_path, 'w', newline='', encoding='utf8') as garbage_file:
        output_writer = csv.writer(output_file)
        garbage_writer = csv.writer(garbage_file)

        # Write the header to the output file
        header = ["login_id", "mail_address", "password", "salt", "birthday", "gender"]
        output_writer.writerow(header)

        # Open input file for reading
        with open(input_file_path, 'r', encoding='utf8') as input_file:
            for line in input_file:
                # Initialize all fields
                login_id = mail_address = password = birthday = salt = gender = None

                # Split line into parts
                parts = line.strip().split(';')

                # Extract and clean data
                login_id = parts[1].strip() if len(parts) > 1 else None
                login_id = to_lower(login_id) if login_id else None

                mail_address = parts[2].strip() if len(parts) > 2 else None
                mail_address = to_lower(mail_address) if mail_address else None

                password = parts[3].strip() if len(parts) > 3 else None
                password = to_lower(password) if password else None

                salt = parts[5].strip() if len(parts) > 5 else None
                salt = to_lower(salt) if salt else None

                birthday = parts[6].strip() if len(parts) > 6 else None
                if birthday and birthday != "birthday_on":  # Ensure it's not the header or an invalid value
                  birthday = convert_date_format(birthday)
                else:
                  birthday = None

                gender = parts[7].strip() if len(parts) > 7 else None
                gender = to_lower(gender) if gender else None

                # Validation checks
                if login_id and validate_login_id(login_id):
                    if mail_address and not validate_mail_address(mail_address):
                        garbage_writer.writerow([line.strip()])
                        continue  # Skip the line if mail address is invalid
                else:
                    garbage_writer.writerow([line.strip()])
                    continue  # Skip the line if login_id is missing or invalid

                # Write valid data to the output file
                output_writer.writerow([login_id, mail_address, password, salt, birthday, gender])

    print(f"Conversion completed for {filename}. Valid data saved to {filename}_structured.csv. Invalid data saved to {filename}_garbage.csv.")

# Example usage
input_folder = "/content/drive/MyDrive/Protexxa/Life-Bear/output"
output_folder = "/content/drive/MyDrive/Protexxa/Life-Bear/output/processed"
csv_files = [file for file in os.listdir(input_folder) if file.endswith(".csv")]

# Process each CSV file in the folder
for csv_file in csv_files:
    input_file_path = os.path.join(input_folder, csv_file)
    process_file(input_file_path, output_folder)

def remove_empty_lines(input_file, output_file):
    try:
        with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
            for line in infile:
                # Strip leading/trailing whitespace and check if line is not empty
                if line.strip() and ',,,,' not in line and 'NULL,,,NULL,' not in line:
                    outfile.write(line)

        print(f"Lines with specified text removed successfully from {input_file}!")
    except Exception as e:
        print(f"An error occurred with file {input_file}: {e}")


# Specify the folder containing the CSV files
input_folder = "/content/drive/MyDrive/Protexxa/Life-Bear/output/processed"
# Specify the folder where the output files will be saved
output_folder = "/content/drive/MyDrive/Protexxa/Life-Bear/output/processed/removed_empty_lines"
# List all CSV files in the input folder
csv_files = [file for file in os.listdir(input_folder) if file.endswith(".csv")]

# Process each CSV file in the folder
for csv_file in csv_files:
    input_file_path = os.path.join(input_folder, csv_file)
    output_file_path = os.path.join(output_folder, csv_file)  # Ensure the output file path includes the original file name
    remove_empty_lines(input_file_path, output_file_path)

# Define the folder containing the processed CSV files
input_folder = "/content/drive/MyDrive/Protexxa/Life-Bear/output/processed/removed_empty_lines"

# Define the output path for the combined CSV file
output_file_path = "/content/drive/MyDrive/Protexxa/Life-Bear/output/processed/FinalSubmission.csv"

# Initialize an empty list to store the DataFrames from each CSV file
dfs = []

# Iterate through the CSV files in the input folder
for filename in os.listdir(input_folder):
    if filename.endswith(".csv") and "garbage" not in filename and "FinalSubmission" not in filename:
        file_path = os.path.join(input_folder, filename)
        try:
            # Read the CSV file into a Pandas DataFrame
            df = pd.read_csv(file_path)
            # Append the DataFrame to the list
            dfs.append(df)
        except Exception as e:
            print(f"Error reading file {filename}: {e}")

# Concatenate all the DataFrames in the list into a single DataFrame
combined_df = pd.concat(dfs, ignore_index=True)

# Save the combined DataFrame to a CSV file
combined_df.to_csv(output_file_path, index=False)

print(f"Combined CSV file saved to: {output_file_path}")

# Define the path for the garbage CSV file
garbage_csv_path = "/content/drive/MyDrive/Protexxa/Life-Bear/output/processed/FinalSubmission_garbage.csv"

# Define the input folder for garbage files
garbage_folder = "/content/drive/MyDrive/Protexxa/Life-Bear/output/processed/garbage"

# Initialize an empty list to store the DataFrames from each garbage CSV file
garbage_dfs = []

# Iterate through the garbage CSV files in the garbage folder
for filename in os.listdir(garbage_folder):
    if filename.endswith(".csv"):
        file_path = os.path.join(garbage_folder, filename)
        try:
            # Read the CSV file into a Pandas DataFrame
            df = pd.read_csv(file_path)
            # Append the DataFrame to the list
            garbage_dfs.append(df)
        except Exception as e:
            print(f"Error reading garbage file {filename}: {e}")

# Concatenate all the garbage DataFrames in the list into a single DataFrame
combined_garbage_df = pd.concat(garbage_dfs, ignore_index=True)

# Save the combined garbage DataFrame to a CSV file
combined_garbage_df.to_csv(garbage_csv_path, index=False)

print(f"Combined garbage CSV file saved to: {garbage_csv_path}")