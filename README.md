# Raising Daisies – Auto-Scan Add-On

This worker scans the internet for healing events and support groups, then sends them to your backend.

## Setup on Railway
1. Go to your **existing project** (with the backend).
2. Click **+ Add Service** → upload this zip.
3. Add Variables:
   - BACKEND_API_URL = your backend URL (like https://yourapp.up.railway.app)
   - ADMIN_TOKEN = same token as backend
4. Edit providers.yaml with your real sources (ICS calendars or JSON-LD event pages).
5. Add a Cron Job to run every 2–4 hours:
   - Command: `python scanner.py`
