# WIRP (World Interest Rate Probabilities)

A tool for tracking and visualizing central bank meeting dates, current policy rates, and implied market expectations across G10 and key Asian economies.

## Overview

WIRP scrapes live data from official central bank sources and injects it into a clean, interactive HTML dashboard (`wirp.html`). It covers markets including:

- **Americas:** US (Fed), Canada (BOC)
- **Europe:** EU (ECB), UK (BOE)
- **Asia-Pacific:** Australia (RBA)

## Features

- **Automated Data Updates:** `update_data.py` scrapes meeting dates and policy rates.
- **Interactive Dashboard:** `wirp.html` provides a visual representation of market expectations.
- **Methodology Documentation:** `make_methodology_doc.py` generates the `WIRP_Methodology.docx` file.
- **VM Deployment:** Includes scripts for deploying to an Oracle VM and setting up an Nginx web server.

## Installation & Setup

### Local Setup

1. **Clone the repository:**
   ```bash
   git clone <your-repository-url>
   cd WIRP
   ```

2. **Install dependencies:**
   ```bash
   pip install requests beautifulsoup4 python-docx openpyxl
   ```

3. **Update data:**
   ```bash
   python update_data.py
   ```

### Deployment to VM

1. **Transfer files to the VM:**
   Use `push_to_vm.sh` to transfer the project files to your server.

2. **Run the deployment script on the VM:**
   ```bash
   bash ~/wirp/deploy_vm.sh
   ```
   This script handles system updates, Python virtual environment setup, Nginx configuration, and cron job scheduling.

## Project Structure

- `update_data.py`: Main data scraping and injection script.
- `wirp.html`: The interactive web dashboard.
- `make_methodology_doc.py`: Script to generate methodology documentation.
- `deploy_vm.sh`: Server-side deployment and setup script.
- `push_to_vm.sh`: Client-side script to sync files with the server.
- `WIRP_Methodology.docx`: Documentation of the calculation methodology.

## License

[Add License Info Here]
