# GVSU-CIS660-Project

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
| matplotlib | 3.10.0  |
| requests   | 2.31.0  |
| warnings   | 3.12.1  |
| sqlite3    | 3.41.2  |
| zipfile    | 3.12.1  |
| logging    | 3.12.1  |
| pandas     | 2.2.3   |
| numpy      | 2.2.2   |
| os         | 3.12.1  |

# Run pipeline

Running the pipeline can be done simply by running the following command:
```
python pipeline.py
```
