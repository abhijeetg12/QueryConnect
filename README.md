# Query Connect - Interactive E-commerce Data Explorer

Query Connect is a Streamlit-based web application that enables users to explore and analyze e-commerce data using natural language queries. By combining Google Cloud's BigQuery for data storage and Vertex AI's Gemini model for natural language processing, it provides an intuitive interface for data analysis.

## Features

- Natural Language Interface with Gemini AI
- Interactive Data Visualization
- Comprehensive Data Analysis 
- Built-in Example Questions
- Data Quality Checks
- Precise SQL Query Generation

## Gemini Function Calling System

The application leverages Vertex AI's Gemini model to translate natural language queries into structured database operations through a set of predefined functions:

### Core Functions

1. **Dataset Management**
   - List Datasets: Retrieves available datasets
   - List Tables: Shows tables within a dataset 
   - Get Table Info: Provides table schema and description

2. **Data Analysis**
   - SQL Query: Executes database queries with smart date handling
   - Table Analysis: Performs distribution and quality analysis
   - Data Quality Checks: Validates data integrity

3. **Visualization**
   - Generates appropriate visualizations based on data type
   - Supports time series, distribution, correlation, and geographic plots

### Function Flow Process

When a user submits a query, the system:
1. Processes the natural language input
2. Determines relevant functions to call
3. Executes functions in sequence
4. Combines results into a comprehensive response
5. Generates appropriate visualizations

## Prerequisites

- Google Cloud Platform Account with:
  - Vertex AI enabled
  - BigQuery enabled
  - BigQuery Data Transfer enabled
- Python 3.7 or higher
- Required Python packages listed in requirements.txt

## Setup Instructions

1. Clone the repository
2. Set up Google Cloud credentials in .streamlit/secrets.toml
3. Run the setup script
4. Launch the Streamlit application

## Dataset Overview

The application uses TheLook E-commerce dataset, which includes:

### Core Tables
- Orders: Transaction details and status
- Order Items: Individual purchase items
- Products: Product catalog
- Users: Customer information
- Inventory Items: Stock tracking
- Distribution Centers: Warehouse data

## Usage Examples

### Business Analytics
- Revenue analysis by product category
- Top-selling products identification
- Average order value trends

### Customer Insights
- Customer lifetime value analysis
- Purchase pattern analysis
- Regional performance metrics

### Inventory Management
- Stock level monitoring
- Order processing time analysis
- Return rate tracking

## Error Handling

The system includes comprehensive error management for:
- Invalid queries
- API failures
- Data access issues
- Visualization errors

Each error returns user-friendly messages with clear next steps.

## Security Features

- Secure credential management via Streamlit secrets
- Service account authentication
- Query sanitization
- Resource usage limits

## Limitations

- Query size limited to 100MB
- Automatic visualization type selection
- Limited to TheLook E-commerce dataset
- Specific Google Cloud Platform requirements

## Performance Considerations

- Query optimization for large datasets
- Efficient date handling
- Resource usage monitoring
- Response time optimization

## Contributing Guidelines

To contribute to the project:
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request
5. Follow code style guidelines

## Support

For technical support or feature requests, please create an issue in the repository. For detailed documentation on the Gemini model and BigQuery integration, refer to the official Google Cloud documentation.
