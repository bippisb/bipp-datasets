Formal Employment (Payroll Reporting in India)
==============================

Employment related statistics published by MOSPI in monlthy 'Payroll Reporting in India' reports.
Example report: [Payroll Reporting in India - December 2022](https://www.mospi.gov.in/sites/default/files/press_release/PayrollReporting-in-India-AnEmploymentPerspective-December2022__24022023.pdf)

The report for the last month contains the tabular information for all the months of the financial year. Currently, only march 2022 and december 2022 reports are used to extract information for the period between april 2021 to december 2022. It's because no other reports are available on the press releases section of MOSPI website.
`extract_payroll` notebook extracts the data from a pdf payroll report in wide (melted) format with 5 columns: age, gender, head, sector, value.
This data is stored in the interim data folder to be later used by `consolidate` notebook to export the final clean consolidated data into two formats: csv for external use and parquet for data engineering use cases.

Project Structure
------------

    ├── Makefile           <- Makefile with commands like `make data` or `make train`
    ├── README.md          <- The top-level README for developers using this project.
    ├── data
    │   ├── external       <- Data from third party sources.
    │   ├── interim        <- Intermediate data that has been transformed.
    │   ├── processed      <- The final, canonical data sets for modeling.
    │   └── raw            <- The original, immutable data dump.
    │
    ├── docs               <- Documentation site if available will be generated using [Docz](https://www.docz.site/)
    │
    ├── models             <- Trained and serialized models, model predictions, or model summaries
    │
    ├── notebooks          <- Jupyter notebooks. Naming convention is a number (for ordering),
    │                         the creator's initials, and a short `-` delimited description, e.g.
    │                         `1.0-mospi-initial-data-exploration`.
    │
    ├── references         <- Data dictionaries, manuals, and all other explanatory materials.
    │
    ├── reports            <- Generated analysis as HTML, PDF, LaTeX, etc.
    │   └── figures        <- Generated graphics and figures to be used in reporting
    │
    ├── pyproject.toml     <- Configuration file to store build system requirements for Python projects
    │
    ├── src                <- Source code for use in this project.
    │   ├── __init__.py    <- Makes src a Python module
    │   │
    │   ├── data           <- Scripts to download or generate data
    │   │   └── make_dataset.py
    │
    ├── features           <- Scripts to turn raw data into features for modeling
    │   └── build_features.py
    │
    ├── models             <- Scripts to train models and then use trained models to make
    │   │                 predictions
    │   ├── predict_model.py
    │   └── train_model.py
    │
    ├── visualization      <- Scripts to create exploratory and results oriented visualizations
    │   └── visualize.py
    │
    └── Dockerfile         <- Dockerfile to run the project as a container
    
--------

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>
