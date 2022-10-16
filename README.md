# Generic Buy Now, Pay Later Project (Group 19)

[![Pyspark](https://img.shields.io/badge/Pyspark-v3.3.0-blue.svg)](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)

## Student name & ID
- Youngjun Cho (1075878)
- Xavier Travers (1178369)
- Glendon Yong Zhen Goh (1145454)
- Ming Hui Tan (1087948)
- Ke He (1068040)

# Project Objectives
We have built a robost ETL pipeline which automatically process the data and generate a ranked list of merchants to consider doing business with.

# Setup

1. Open up your terminal or command prompt and cd to the root directory of the project folder.
2. Run the following command to install all required python packages (**May take a while to install**):
    ```
    pip install -r requirements.txt
    ```
3. When done, run the following command:
    ```
    python3 ./scripts/run_all.py
    ```

# Scripts
The python scripts are stored under the `scripts` folder.

To retrain the model without re-running the entire scripts:
```
python3 ./scripts/fraud_modelling_script.py
```

To run the ETL pipeline only without re-running the entire scripts:
```
python3 ./scripts/etl_script.py
```

The ETL script can be run with arguments (optional):

- `'-d', '--debug', '--debugging'`: print the debug statements
- `'-i', '--input'`: specify where the input data will be stored
- `'-m', '--model'` : specify where the model will be stored
- `'-o', '--output'`: specify where the ranking results will be stored


# Notebooks
The jupyter notebooks are under `notebook` folder.

`deprecated research and methods` folder contains methods that the team has researched and experimented. These methods were not implemented in the final pipeline. 

`Project Summary Notebook.ipynb` jupyter notebook details the various processes, challenges and findings we had in the development process. For optimal reading experience, please view the notebook via jupyter notebook.


# External Datasets
External datasets are under `data/tables`. 

The ABS (Australian Bureau of Statistics) datasets are small and are originally downloaded in `.zip` format from the website. Hence instead of automating the process of downloading the data, we manually download the dataset, unzip it and store them under `data/tables`.

There are mainly three external dataset that we used:

- `data/tables/POA` : 2021 demographic data by Australian postal areas

- `data/tables/SA2` : 2021 demographic data by Australian SA2 areas

These two dataset can be downloaded [here](https://www.abs.gov.au/census/find-census-data/datapacks).

In addition, an external mapping file is **automatically downloaded** from the internet to map the postal area to SA2 areas.

The dataset can be downloaded externally [here](https://www.matthewproctor.com/Content/postcodes/australian_postcodes.csv) for reference and the data dictionary [here](https://www.matthewproctor.com/australian_postcodes).

# Curated Data and Outputs

The curated data are stored in `data/curated`.

The final rankings are stored in `model` by default. 


# Model

The model used for this project is stored in the `model` folder.
To retrain the model, please reference to the command in `scripts` session

# Used Python Libraries

See `requirements.txt` under the root folder.
