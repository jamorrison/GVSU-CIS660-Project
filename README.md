# GVSU-CIS660-Project

This project looks at a dataset of
[hockey statistics from Kaggle](https://www.kaggle.com/datasets/open-source-sports/professional-hockey-database). It
pulls the data down, extracts the necessary CSV files, transforms the data into a usable dataset, loads the data into
a SQLite database, and then outputs a few interesting statistics and visualizations.

## Install dependencies via conda

All dependencies for the project can be installed via [conda](https://anaconda.org).
```
conda env create -f environment.yml
```

### Python Libraries Used

If you aren't able to use `conda`, then the following dependencies will need to be installed. All should, theoretically,
be available via `pip`.

|  Library   | Version |
|:----------:|:-------:|
| python     | 3.12.1  |
| matplotlib | 3.10.0  |
| requests   | 2.31.0  |
| warnings   | 3.12.1  |
| sqlite3    | 3.41.2  |
| zipfile    | 3.12.1  |
| logging    | 3.12.1  |
| pandas     | 2.2.3   |
| numpy      | 2.2.2   |
| os         | 3.12.1  |

## Run pipeline

Running the pipeline can be done simply by running the following command:
```
python pipeline.py
```

## Output

A successful run of the pipeline should output several logging messages, plus two files:
```
data_visualizations.pdf
data_warehouse.sqlite
```

The PDF includes the generated data visualizations, while the `.sqlite` file is the SQLite database with the loaded
data.
