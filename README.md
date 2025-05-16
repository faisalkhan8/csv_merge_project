# Federal Audit Clearinghouse (FAC) Data Integration Pipeline

A professional, efficient pipeline for fetching and merging Federal Audit Clearinghouse data from SAM.gov APIs. This tool combines multiple FAC datasets into a single, coherent dataset using report_id as the primary key.

## Features

- 🔄 Automatic data alignment using report_id
- 🚀 Memory-efficient streaming processing
- ✅ Automatic schema validation
- 📊 Progress tracking and detailed logging
- 🔁 Error handling with retry logic
- 🧹 Automatic cleanup of temporary files
- 🔑 Secure API key management via environment variables

## Prerequisites

- Python 3.8 or higher
- SAM.gov API key (register at https://sam.gov/data-services/)
- Sufficient disk space for temporary files

- **Zero Data Storage:** No data files in repository, everything fetched on demand
- **Environment-Based:** Configuration through environment variables
- **Memory Efficient:** In-memory processing where possible
- **Clean & Light:** Automatic cleanup of temporary files
- **Fast:** Optimized API calls and minimal disk I/O

-   **Configuration-Driven:** Define data sources, join keys, and output settings in `config.yaml`.
-   **Flexible Sources:** Supports paginated APIs, direct CSV downloads, and local CSV files.
-   **Robust Downloads:** Implements retries for network requests and progress bars for user feedback.
-   **Efficient Merging:** Utilizes DuckDB for fast, out-of-core merging of potentially large datasets.
-   **Standardized Output:** Produces a GZIP-compressed CSV as the final merged dataset.
-   **Clean Repository:** The Git repository contains only code and example configuration. Data is fetched/generated locally and ignored by Git.

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/yourusername/fac-data-pipeline.git
    cd fac-data-pipeline
    ```

2. Install dependencies:

    ```bash
    pip install -r requirements.txt
    ```

3. Create a .env file:

    ```bash
    cp .env.example .env
    # Edit .env with your SAM.gov API key
    ```

## Configuration

The pipeline is configured through `config.yaml`. Key settings include:

```yaml
settings:
  output_filename: merged_fac_data.csv.gz
  download_directory: downloaded_csvs
  primary_join_key: report_id
  api_page_size: 1000
```

## Usage

1. Ensure your API key is set in .env:

    ```bash
    SAM_API_KEY=your_api_key_here
    ```

2. Run the pipeline:

    ```bash
    python run_pipeline.py
    ```

3. Find the merged data in your specified output file.

## Data Structure

The pipeline merges the following FAC datasets using `report_id` as the key:

- General information
- Findings
- Findings text
- Corrective action plans

## Error Handling

The pipeline includes comprehensive error handling:

- Automatic retries for API failures
- Detailed logging to pipeline.log
- Validation of input data
- Cleanup of temporary files

## Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) first.

1. Fork the repository
2. Create your feature branch: `git checkout -b feature/amazing-feature`
3. Commit your changes: `git commit -m 'Add amazing feature'`
4. Push to the branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- SAM.gov for providing the Federal Audit Clearinghouse API
- DuckDB team for the excellent database engine
- All contributors to this project

## Project Status Update - May 17, 2025

I have successfully created and published a professional data pipeline for handling Federal Audit Clearinghouse (FAC) data. The repository is now publicly available on GitHub:

📦 **Repository**: [https://github.com/faisalkhan8/csv_merge_project](https://github.com/faisalkhan8/csv_merge_project)

### Key Components:
- `run_pipeline.py`: Main script with memory-efficient data processing
- `test_endpoints.py`: API endpoint testing utility
- `config.yaml`: Configuration for API endpoints and processing parameters
- `tests/`: Comprehensive test suite
- Documentation: README.md, CONTRIBUTING.md, LICENSE

### Features Implemented:
- Memory-efficient processing for large CSV files (500MB+)
- Streaming API data fetching
- DuckDB-based efficient data merging
- Automated cleanup of temporary files
- Comprehensive error handling

### Getting Started:
1. Clone the repository
2. Install dependencies from requirements.txt
3. Add your SAM.gov API key to .env
4. Run the pipeline!

The code is now open source and available for anyone to use, contribute to, or adapt for their needs.