```markdown
# DLD Transaction Data Uploader

This script fetches Dubai Land Department (DLD) transaction data from the open data gateway, processes it, and uploads the results to Azure Blob Storage. It also includes error handling and sends an email notification in case of any issues.

## Requirements
- Python 3.x
- Required Python packages: pandas, azure-storage-blob, requests, smtplib, email.mime

## Environment Variables
Ensure the following environment variables are set:
- `email_pw`: Office 365 email password
- `azure_account_key`: Azure Blob Storage account key
- `azure_account_name`: Azure Blob Storage account name
- `azure_blob_name`: Azure Blob Storage blob name
- `azure_container_name`: Azure Blob Storage container name

## Usage
1. Install the required Python packages using `pip install pandas azure-storage-blob requests`.
2. Set the necessary environment variables.
3. Run the script using `python script_name.py`.

## Script Structure

### Modules and Libraries
- `pandas`: Library for data manipulation and analysis.
- `azure.storage.blob`: Azure Blob Storage library for interacting with Azure Blob Storage.
- `requests`: Library for making HTTP requests.
- `smtplib`: Library for sending email messages.
- `email.mime`: MIME (Multipurpose Internet Mail Extensions) library for handling email-related functionality.
- `os`: Module providing a way of using operating system-dependent functionality.

### Script Functions
1. **sendUnsuccessEmail**: Sends an email notification for unsuccessful operations.
2. **fetch_dld_transaction**: Fetches DLD transaction data from the open data gateway.
3. **update_blob**: Uploads processed data to Azure Blob Storage.

## Running the Script
1. Clone the repository or download the script.
2. Navigate to the script directory.
3. Set the required environment variables.
4. Open a terminal or command prompt.
5. Run the script using `python script_name.py`.

## Author
- Aathil Ahamed
  - LinkedIn: [aathilks](https://www.linkedin.com/in/aathilks/)
  - Email: atldeae@gmail.com

## License
This project is licensed under the [MIT License](LICENSE).
```
