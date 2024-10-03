import csv
import time
from get_data import get_company_data
import pandas as pd
import keyboard
import threading

ADR_JUDET_FILTER = 'Dâmbovița'
running = True

# Path to your CSV file
csv_file_path = '3firme_neradiate_cu_sediu_2024-07-07.csv'
already_processed_file_path = 'already_processed.xlsx'
errors_file_path = 'errors.xlsx'
no_email_file_path = 'no_email.xlsx'
company_data_array_file_path = '3firme_neradiate_cu_sediu_2024-07-07.xlsx'

# Initiate an array for storing the company dataqqq
company_data_array = []

try:
    df = pd.read_excel(company_data_array_file_path)
    company_data_array = df.to_dict('records')
except FileNotFoundError:
    print("No company_data_array file found")

# Open the already_processed file and read the CUIsq
already_processed = []

# Check if the file exists
try:
    df = pd.read_excel(already_processed_file_path, engine='openpyxl')
    already_processed = df[0].tolist()
    already_processed = [str(ap) for ap in already_processed]
except FileNotFoundError:
    print("No already_processed file found")

no_email = []

# Open the no_email file and read the CUIsqqq
try:
    df = pd.read_excel(no_email_file_path)
    no_email = df[0].tolist()
    no_email = [str(no) for no in no_email]
except FileNotFoundError:
    print("No no_email file found")


errors = []

# Open the errors file and read the CUIsq
try:
    df = pd.read_excel(errors_file_path)
    errors = df[0].tolist()
    errors = [str(error) for error in errors]
except FileNotFoundError:
    print("No errors file found")

print(f"Already processed: {already_processed}")


def listen_for_keypress():
    global running
    # Listen for the 'q' key to be pressed
    keyboard.wait('q')
    running = False


def extract_data():
    # Open and read the CSV file
    with open(csv_file_path, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file, delimiter='^')
        # Iterate through the rows
        counter = 0
        no_email_counter = 0
        for i, row in enumerate(csv_reader):
            # Check if the program should stop
            if not running:
                break

            # Check if the company has already been processedq
            if row['CUI'] in already_processed:
                print(f"Company {i+1} with CUI {row['CUI']} has already been processed")
                continue

            # Check if the company has no emailq
            if row['CUI'] in no_email:
                print(f"Company {i+1} with CUI {row['CUI']} has no email")
                continue

            # Filter the rows by the desired countyqq
            if ADR_JUDET_FILTER in row['ADR_JUDET']:
                # Get the company CUI
                company_cui = row['CUI']

                try:
                    # Get the company data
                    company_data = get_company_data(company_cui)
                    # Sleep for 1 secod to avoid getting blockedqq
                    time.sleep(0.1)
                except Exception as e:
                    try:
                        print(f"Failed to get data for company with CUI {company_cui}: {e}")
                        print("Retrying in 1 second...")
                        time.sleep(0.1)
                        # Retry getting the company data
                        company_data = get_company_data(company_cui)
                        # Sleep for 1 secod to avoid getting blocked
                        time.sleep(0.1)
                    except Exception as e:
                        print(f"Failed to get data for company with CUI {company_cui}: {e}")
                        errors.append(str(company_cui))
                        # Write errors to an xlsx fileq
                        df = pd.DataFrame(errors)
                        df.to_excel('errors.xlsx', index=False)
                        continue
                
                # Extract the first autocomplete elementq
                first_autocomplete = company_data['data']['autocomplete'][0]

                # Extract the primaryEmail
                primary_email = first_autocomplete['primaryEmail']

                if not primary_email:
                    no_email_counter += 1
                    print(f"Company {i+1} with CUI {company_cui} has no primaryEmail")
                    no_email.append(str(company_cui))
                    if no_email_counter % 10 == 0:
                        # Write no_email to an xlsx fileq
                        df = pd.DataFrame(no_email)
                        df.to_excel('no_email.xlsx', index=False)
                        continue
                

                # Extract the last CAEN
                last_caen = first_autocomplete['caen'][-1]

                # Create a row with the complete and updated dataq
                row = {
                    **row,
                    'primaryEmail': primary_email,
                    'lastCAEN': last_caen
                }

                print(f"Company {i+1} with CUI {company_cui} has primaryEmail {primary_email} and lastCAEN {last_caen}")

                counter += 1
                
                company_data_array.append(row)
                already_processed.append(str(company_cui))
                
                
                if counter % 10 == 0:
                    # Write all the data from company_data_array to a new xlsx fileq
                    df = pd.DataFrame(company_data_array)
                    df.to_excel(company_data_array_file_path, index=False)
                    
                    # Write already_processed to an xlsx file
                    df = pd.DataFrame(already_processed)
                    df.to_excel('already_processed.xlsx', index=False)





if __name__ == '__main__':
    # Start the keypress listener thread
    keypress_thread = threading.Thread(target=listen_for_keypress)
    keypress_thread.start()

    # Extract the data
    extract_data()

    # Wait for the keypress listener thread to finish
    keypress_thread.join()

    print("Program terminated.")

