## data/ Directory

While this project is designed with cloud-first deployment in mind, the data/ directory serves as a local staging area during development and testing.

    It includes:

        data/raw/ – for extracted data files (e.g., GeoJSON, shapefiles)

        data/processed/ – for transformed outputs ready for loading or export

This local storage is especially useful when iterating on pipeline logic, validating data transformations, or running tests without relying on cloud resources.

    Note: Use the config.yaml file to toggle between local and cloud storage:

storage:
  mode: local  # options: local, s3

When mode is set to s3, the pipeline bypasses this folder and interacts directly with cloud storage (e.g., AWS S3).