# Dashboard Project

This project processes Conviva viewing data, Opta match data, and Oracle match content data to create a **datamart** in BigQuery. The datamart is visualized using Looker Studio to build an interactive dashboard.

## Project Structure

The project consists of four main Python scripts:

1. **Conviva Data**
   - Retrieves viewing data from the Conviva platform.
   - Transforms the data for analysis.
   - Loads the processed data into BigQuery.

2. **Opta Data**
   - Fetches match data from the Opta API.
   - Cleans and processes the data.
   - Loads the data into BigQuery.

3. **Oracle Data**
   - Extracts match content from an Oracle database.
   - Performs data transformations.
   - Loads the processed data into BigQuery.

4. **Datamart**
   - Combines data from Conviva, Opta, and Oracle into a unified BigQuery datamart.
   - Optimizes the datamart for analysis and reporting.

### Dashboard
- The datamart in BigQuery is connected to Looker Studio.
- The dashboard provides insights into viewing data and match performance metrics.

