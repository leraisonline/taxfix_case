# Data Pipeline: Taxfix Case

This pipeline:
- fetches data from an API,
- processes it (validatation, cleaning, anonymization),
- stores it in SQLite database,
- generates a report based on the processed data.

## Prerequisites

Installed Docker.

## Project Structure

```
taxfix_case/
├── src/
│   ├── data_processor.py
│   └── report_generator.py
├── tests/
│   └── test_data_processor.py
├── reports/
├── Dockerfile
├── README.md
├── README-solution.md
└── requirements.txt
```

## Building the Docker Image

1. Clone this repository to your local machine.
2. Go to the project directory in your terminal.
3. Build the Docker image using the following command:

```bash
docker build -t taxfix-data-process-report .
```

This command builds a Docker image, names it "data-processor-reporter", and runs the tests during the build process.

## Running the Container

After building the image, you can run the container using the following command:

```bash
docker run -v $(pwd)/reports:/app/reports taxfix-data-process-report
```

This command does the following:
- Starts a new container from the "data-process-report" image.
- Installs the `reports` directory from your current working directory to `/app/reports` in the container.
- Runs the data processing and report generation scripts.

## Running Tests

Tests are automatically run during the Docker image building. If you want to run them manually, you can use the following command:

```bash
docker run taxfix-data-process-report pytest 
```

## Accessing the Results

After the container finishes running, you can find the generated report in the `reports` directory on your host machine.

The processed data is stored in a SQLite database file inside the container under the name `persons.sqlite`.

## Customization

You can modify the `CONFIG` dictionary in Python scripts to set parameters like API URL, database path, and output paths.

## Troubleshooting

If you encounter any issues, you can check the Docker logs:

```bash
docker logs $(docker ps -lq)
```