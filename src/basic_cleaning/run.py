#!/usr/bin/env python
"""
Performs basic cleaning on the data and saves the results in W&B
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def basic_data_cleaning(args):
    """Runs the data cleaning script with the provided arguments"""

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info("Downloading and reading input artifact")
    artifact = run.use_artifact(args.input_artifact)
    artifact_path = artifact.file()
    df_data_to_clean = pd.read_csv(artifact_path)
    logger.info("Input artifact downloaded and read")
    idx = df_data_to_clean['price'].between(args.min_price, args.max_price) & df_data_to_clean['longitude'].between(-74.25, -73.50) & df_data_to_clean['latitude'].between(40.5, 41.2)
    df_data_to_clean = df_data_to_clean[idx].copy()
    # Convert last_review to datetime
    df_data_to_clean['last_review'] = pd.to_datetime(df_data_to_clean['last_review'])

    # Save the cleaned data to a new artifact
    df_data_to_clean.to_csv("clean_sample.csv", index=False)
    logger.info("Saving cleaned data to output artifact")
    output_artifact = wandb.Artifact(args.output_artifact, type=args.output_type, description=args.output_description)
    output_artifact.add_file("clean_sample.csv")
    run.log_artifact(output_artifact)
    logger.info("Cleaned data saved to output artifact")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This step cleans the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Input artifact to use",
        required=True,
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Output artifact to create",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Output type (e.g. csv, parquet)",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price for filtering",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price for filtering",
        required=True
    )


    args = parser.parse_args()

    basic_data_cleaning(args)
