import requests
import time
from datetime import datetime
import pandas as pd

# Load the Excel file
file_path = ''  # Replace with the path to your Excel file

# Read the Excel file (if your file has multiple sheets, specify the sheet name)
df = pd.read_excel(file_path, sheet_name=0)  # You can replace 0 with the sheet name if needed

# Assuming the contact IDs are in a column named 'Contact ID'
contact_ids = df['Contact ID'].astype(str).tolist()  # Convert to string to ensure IDs are properly formatted

API_KEY = ''  # Replace with your actual HubSpot API key
BASE_URL = 'https://api.hubapi.com'

# API URLs
search_url = f'{BASE_URL}/crm/v3/objects/deals/search'
contact_update_url = f'{BASE_URL}/crm/v3/objects/contacts'
bulk_update_url = f'{BASE_URL}/crm/v3/objects/deals/batch/update'

headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}

#
all_deals = []  # To store all deals
after = None  # Used for pagination
                                                         # for sandbox

# Below are options for Production
closed_won_deals=[]  # Add closed Won Deal stages name . Will contains more than one values if you have multiple pipelines


def parse_closedate(closedate):
    """
    Parse the closedate string into a datetime object, handling multiple formats.
    """
    for fmt in ('%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%SZ'):
        try:
            return datetime.strptime(closedate, fmt)
        except ValueError:
            continue
    raise ValueError(f"Date format not supported: {closedate}")

# Fetch all deals associated with the list of contact IDs
for contact_id in contact_ids:
    print(f"Processing contact ID: {contact_id}")
    all_deals.clear()  # Reset all deals for each contact
    after = None  # Reset pagination variable for each contact

    while True:
        # Build the request payload
        data = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "associations.contact",
                    "operator": "EQ",
                    "value": contact_id
                }]
            }],
            "limit": 100  # Request up to 100 records per page
        }

        # Add paging information if present
        if after:
            data["after"] = after

        # Make the API request
        try:
            response = requests.post(search_url, headers=headers, json=data)
            response.raise_for_status()  # Raise an error for bad HTTP status codes
            response_data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error fetching deals for contact {contact_id}: {e}")
            break  # Exit loop on error

        # Collect the deals from this response
        all_deals.extend(response_data.get('results', []))

        # Check if there are more pages to fetch
        paging_info = response_data.get('paging', {}).get('next', {})
        after = paging_info.get('after')  # Update the `after` value for the next page
        if not after:
            break  # Exit loop if there are no more pages

    # Extract IDs and close dates for deals with stages in the closed_won_deals list
    closed_won_records = []
    for deal in all_deals:
        dealstage = deal['properties'].get('dealstage')
        closedate = deal['properties'].get('closedate')

        if dealstage in closed_won_deals and closedate:
            try:
                parsed_date = parse_closedate(closedate)
                closed_won_records.append({
                    "id": deal['id'],
                    "closedate": parsed_date
                })
            except ValueError as e:
                print(f"Skipping deal {deal['id']} due to date parsing error: {e}")

    # Sort the records by close date in ascending order
    closed_won_records.sort(key=lambda record: record["closedate"])

    # Determine the first closed won date
    contact_first_closed_won_date = (
        closed_won_records[0]["closedate"].strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if closed_won_records else None
    )

    print(f"Number of closed won deals for contact {contact_id}: {len(closed_won_records)}")
    print(f"First closed won date for contact {contact_id}: {contact_first_closed_won_date}")

    # Update the contact's property in HubSpot
    if contact_first_closed_won_date:
        contact_update_payload = {
            "properties": {
                "first_closed_won_deal_date": contact_first_closed_won_date
            }
        }
        try:
            contact_update_response = requests.patch(
                f"{contact_update_url}/{contact_id}",
                headers=headers,
                json=contact_update_payload
            )
            contact_update_response.raise_for_status()  # Raise an error for bad HTTP status codes
            print(f"Contact {contact_id} updated successfully with first closed won date: {contact_first_closed_won_date}")
        except requests.exceptions.RequestException as e:
            print(f"Error updating contact {contact_id}: {e}")
    else:
        print(f"No closed won deals found for contact {contact_id}. Contact property not updated.")

    # Check if the first closed won date is older or newer than 365 days and update dealtype
    if contact_first_closed_won_date:
        first_closed_date = parse_closedate(contact_first_closed_won_date)
        today = datetime.utcnow()
        date_diff = today - first_closed_date

        # Determine deal type based on the date difference
        deal_type_internal = "Existing Business" if date_diff.days > 365 else "New Business"

        # Bulk update deals in chunks of 100
        chunk_size = 100
        for i in range(0, len(closed_won_records), chunk_size):
            chunk = closed_won_records[i:i + chunk_size]

            bulk_update_payload = {
                "inputs": [
                    {
                        "id": deal['id'],
                        "properties": {
                            "deal_type": deal_type_internal
                        }
                    }
                    for deal in chunk
                ]
            }

            # Debugging: Print the payload
            print(f"Bulk update payload (chunk {i // chunk_size + 1}):", bulk_update_payload)

            # Make the bulk update request
            try:
                bulk_update_response = requests.post(
                    bulk_update_url,
                    headers=headers,
                    json=bulk_update_payload
                )
                bulk_update_response.raise_for_status()  # Raise an error for bad HTTP status codes
                print(f"Updated dealtype for {len(bulk_update_payload['inputs'])} deals to: {deal_type_internal}")
            except requests.exceptions.RequestException as e:
                print(f"Error updating deals for contact {contact_id}: {e}")

            # Delay to prevent throttling
            # time.sleep(1)

    else:
        print(f"No closed won deals found for contact {contact_id}. No deals updated.")

    # Delay between processing each contact to prevent throttling
    time.sleep(1)

print("All contacts processed.")
