Healthcare Eligibility Pipeline (Python)

How to Run the Pipeline

Prerequisites:
Python version 3.8 or higher is required. Install the required dependency by running:
pip install pandas

Input Files:
Place all partner eligibility files inside the data directory.

Running the Pipeline:
Execute the pipeline using the following command:
python pipeline.py

Output:
The pipeline generates a single unified and standardized output file (for example, output/unified_eligibility.csv) containing records from all configured partners.

How to Add a New Partner

This pipeline is configuration-driven and does not require changes to the core Python code.

To add a new partner:

Place the new partner’s eligibility file in the data directory.

Add a new entry in the partner configuration file (for example, partners.json) specifying the file name, delimiter, column mappings to the standard schema, date format if applicable, and a partner code.

Run the pipeline again using:
python pipeline.py

The new partner’s data will automatically be ingested, transformed, and included in the unified output.
