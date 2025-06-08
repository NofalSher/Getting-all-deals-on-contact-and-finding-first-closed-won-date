# HubSpot Deal Updater

This script:
- Retrieves deals associated with HubSpot contacts from an Excel file
- Identifies the first 'Closed Won' deal for each contact
- Updates a custom property on the contact in HubSpot
- Tags deals as "New Business" or "Existing Business" based on the closed date

## Setup

1. Install dependencies:

```bash
pip install pandas requests python-dotenv openpyxl
