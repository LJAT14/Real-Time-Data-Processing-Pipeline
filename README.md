# Real-time Data Processing Pipeline

A comprehensive data processing platform built with Streamlit that demonstrates ETL capabilities, real-time data streaming, and analytics dashboard functionality.

## Overview

This application provides a complete data processing pipeline solution featuring:

- **Real-time Data Processing**: Live data ingestion and transformation
- **ETL Operations**: Extract, Transform, Load capabilities with validation
- **Multiple Data Sources**: File upload, API endpoints, database connections, and mock data streams
- **Monitoring & Analytics**: Comprehensive logging, performance metrics, and data quality monitoring
- **Interactive Configuration**: Customizable pipeline parameters and processing rules

## Features

### Data Sources
- **File Upload**: CSV file processing with validation
- **API Integration**: REST API endpoint connectivity with authentication
- **Database Connections**: SQLite, PostgreSQL, MySQL support
- **Mock Data Streams**: Configurable data generation for testing

### Processing Pipeline
- **Data Validation**: Automated data quality checks and integrity validation
- **Data Cleaning**: Duplicate removal, outlier detection, and missing value handling
- **Data Transformation**: Feature engineering and derived metrics calculation
- **Error Handling**: Configurable error management strategies

### Monitoring & Analytics
- **Real-time Monitoring**: Live pipeline status and performance metrics
- **Processing Logs**: Detailed execution logs with filtering and search
- **Data Quality Metrics**: Completeness, uniqueness, and consistency tracking
- **Performance Analytics**: Throughput analysis and processing time optimization

### Configuration Management
- **Pipeline Settings**: Batch size, processing intervals, and validation rules
- **Export/Import**: Configuration backup and restoration
- **Reset Options**: Data and configuration management tools

## Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Setup Instructions

1. Clone the repository:
```bash
git clone <repository-url>
cd data-processing-pipeline
```

2. Install required dependencies:
```bash
pip install -r requirements.txt
```

3. Run the application:
```bash
streamlit run app.py
```

4. Access the application at (https://larisrealtimedataprocessingpipeline.streamlit.app/)`

## Deployment on Streamlit Cloud

### GitHub Setup
1. Push your code to a GitHub repository
2. Ensure `requirements.txt` is in the root directory
3. Verify the main application file is named `app.py`

### Streamlit Cloud Deployment
1. Visit [share.streamlit.io](https://share.streamlit.io)
2. Connect your GitHub account
3. Select your repository and branch
4. Set the main file path: `app.py`
5. Deploy the application

### Environment Configuration
No additional environment variables required for basic functionality.

## Usage

### Starting the Pipeline
1. Navigate to the "Live Pipeline" tab
2. Configure processing parameters in the sidebar
3. Click "Start Pipeline" to begin real-time processing
4. Monitor performance metrics and logs in real-time

### Data Source Configuration
1. Select "Data Sources" tab
2. Choose your preferred data source type
3. Configure connection parameters
4. Test connectivity and begin processing

### Performance Monitoring
1. Access "Processing Monitor" tab
2. Review processing logs and performance metrics
3. Filter logs by status, step, or time range
4. Analyze throughput and error rates

### Analytics Dashboard
1. View "Analytics" tab for processed data insights
2. Examine data quality metrics and distributions
3. Generate custom visualizations
4. Export results for further analysis

## Technical Architecture

### Data Processing Flow
1. **Data Ingestion**: Multiple source support with standardized interfaces
2. **Validation Layer**: Configurable data quality checks
3. **Cleaning Pipeline**: Automated data cleaning and preprocessing
4. **Transformation Engine**: Feature engineering and enrichment
5. **Output Management**: Processed data storage and export

### Performance Optimization
- **Batch Processing**: Configurable batch sizes for optimal throughput
- **Memory Management**: Efficient data handling for large datasets
- **Error Recovery**: Robust error handling and recovery mechanisms
- **Monitoring**: Comprehensive performance tracking and alerting

## Configuration Options

### Pipeline Parameters
- **Batch Size**: Number of records processed per batch (100-5000)
- **Processing Interval**: Time between processing cycles (1-30 seconds)
- **Auto Cleaning**: Enable/disable automatic data cleaning
- **Data Validation**: Enable/disable validation steps
- **Error Handling**: Skip, stop, or retry on errors

### Data Quality Settings
- **Null Value Threshold**: Maximum acceptable null percentage
- **Duplicate Threshold**: Maximum acceptable duplicate percentage
- **Outlier Detection**: Statistical outlier identification and handling

## Troubleshooting

### Common Issues
1. **Memory Errors**: Reduce batch size or processing interval
2. **Connection Timeouts**: Check network connectivity and API endpoints
3. **Data Format Errors**: Verify input data schema and format
4. **Performance Issues**: Monitor system resources and optimize configuration

### Debug Mode
Enable detailed logging in the sidebar configuration panel for enhanced troubleshooting.

## Contributing

This application is designed for educational and demonstration purposes. To extend functionality:

1. Modify data source connectors in the data sources section
2. Add custom validation rules in the validation functions
3. Implement additional transformation logic in the transform_data function
4. Enhance monitoring capabilities with custom metrics

## License

This project is provided for educational and demonstration purposes. Please refer to your organization's guidelines for commercial usage.

## Support

For technical issues or questions:
1. Review the troubleshooting section
2. Check processing logs for detailed error information
3. Verify configuration settings and data formats

4. Consult the monitoring dashboard for performance insights
