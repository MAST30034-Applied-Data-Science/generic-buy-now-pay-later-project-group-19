# Generic Buy Now, Pay Later Project (Group 19)


## Student name & ID
- Youngjun Cho (1075878)
- Xavier Travers (1178369)
- Glendon Yong Zhen Goh (1145454)
- Ming Hui Tan (1087948)
- Ke He (1068040)

## TODO:Pipeline
Run these commands in the following order to generate a ranked list of merchants to consider doing business with.

### TODO: Scripts
The python scripts are stored under the `scripts` folder.

`etl_script.py` runs the whole ETL process
`fraud_modelling_script.py` produces the model

### Notebooks
The jupyter notebooks are under `notebook` folder.

`Research` folder contains experimental methods that the team has researched and experimented. These methods were not implemented in the pipeline. 

`Draft` folder contains the draft notebooks. The team experiment utilities in this folder before finalizing them.

`Analysis` folder contains finalized notebooks. These notebooks show our insights for the 

## External Datasets
External datasets are under `data/tables`. 

The ABS (Australian Bureau of Statistics) datasets are small and are originally downloaded as `.zip` format from the website. Hence instead of automate the process of downloading the data, we manually download the dataset, unzip it and store them under `data/tables`.

There are mainly two external dataset that we used:

`data/tables/POA` : 2021 demographic data by Australian postal areas
`data/tables/SA2` : 2021 demographic data by Australian SA2 areas

The download link is `https://www.abs.gov.au/census/find-census-data/datapacks`

## Curated Data and Outputs(TODO: final output folder?)

The curated data are stored in `data/curated`
The final rankings are stored in `model`


## Model
The models used for this project are stored in the `model` folder.
To retrain the model, please run the below command under the `root` folder of the working directory.

`python3 ./scripts/fraud_modelling_script.py`



## Used Python Libraries

See `requirements.txt` under the root folder.

