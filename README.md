# Generic Buy Now, Pay Later Project (Group 19)

[![Pyspark](https://img.shields.io/badge/Pyspark-v3.3.0-blue.svg)](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)

# Group Members

Student Name | Student ID 
--- | --- 
Youngjun Cho | 1075878
Xavier Travers | 1178369
Glendon Yong Zhen Goh | 1145454
Ming Hui Tan | 1087948
Ke He | 1068040

# Project Objectives
Build a robust and flexible ETL pipeline which automatically processes merchant and transaction data and generates a ranked list of merchants to partner up with.

# Setup

1. Open up your terminal or command prompt and cd to the root directory of the project folder.
2. Run the following command to install all required python packages (**May take a while to install**):
    ```
    pip3 install -r requirements.txt
    ```
3. When done, run the following command which runs through all the steps necessary to generate rankings in the `./ranking` folder (**May take up to 15 minutes to run depending on device**):
    ```
    python3 ./scripts/run_all.py
    ```

# Scripts
The python scripts are stored under the `scripts` folder.
You must run them from the root directory of this repository unless you explicitly define arguments.
Run any of the scripts below with the `-h` argument to show a help message describing possible arguments.

### Run the scripts in the following order if starting from scratch:

1. To **train** the **consumer fraud model**:
    ```
    python3 ./scripts/fraud_modelling_script.py
    ```

2. To run the **ETL** (this takes a while to run):
    ```
    python3 ./scripts/etl_script.py
    ```

3. To run the **ranking**:
    ```
    python3 ./scripts/rank_script.py
    ```

### *Alternatively,* to run all of the above scripts in one go:
```
python3 ./scripts/run_all.py
```

# Notebooks
The jupyter notebooks are under `notebook` folder.

The `Project Summary Notebook.ipynb` details the various processes, challenges and findings we had in the development process. For optimal reading experience, please view the notebook via jupyter notebook, instead of vscode or other ide.

`deprecated research and methods` folder contains methods that the team has researched and experimented. These methods were not implemented in the final pipeline. 

`project development notebooks` folder contains various notebooks that serve as a ground to test and develop our codes before implementing them into a script.


# External Datasets
External datasets are under `data/tables`. 

The ABS (Australian Bureau of Statistics) datasets are small and download in a `.zip` file format from the website. Instead of automating the process of downloading and unzipping the data, we have manually download the dataset, unzipped it and stored the results under `data/tables`.

There are three external dataset used:

- `data/tables/POA` : 2021 demographic data by Australian postal areas

- `data/tables/SA2` : 2021 demographic data by Australian SA2 areas

These two datasets can be downloaded [here](https://www.abs.gov.au/census/find-census-data/datapacks/download/2021_GCP_all_for_AUS_short-header.zip).

In addition, an external mapping file is **automatically downloaded** from the internet to map the postal area to SA2 areas.

The dataset can be downloaded externally [here](https://www.matthewproctor.com/Content/postcodes/australian_postcodes.csv) for reference and the data dictionary [here](https://www.matthewproctor.com/australian_postcodes).

# Curated Data, Segmentation, and Output

The curated data are stored in `data/curated`.

Decisions related to segmentation of merchants were research based.
They result in the generation of a `segments.json`, which is computed in the `Project Summary Notebook.ipynb`, and is saved to `./ranking`.

The final rankings are stored in the `./ranking` by default. 


# Model

The fraud rate prediction model used for this project is stored in the `model` folder.
This is a simple OLS linear regression.
To train/retrain the model, please see the `scripts` session.

# Used Python Libraries

See `requirements.txt` under the root folder.
