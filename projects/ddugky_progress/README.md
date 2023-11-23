ddugky_progress
==============================

This project aims to capture and analyze the Cumulative Progress of the Deen Dayal Upadhyaya Grameen Kaushalya Yojana (DDUGKY) program. The data is sourced from the official DDUGKY Dashboard available at https://dashboard.rural.nic.in/dashboardnew/ddugky.aspx. The process involves utilizing Selenium for web scraping and Beautiful Soup for data extraction, followed by data processing.The primary data source for this project is the DDUGKY Dashboard, which provides comprehensive information on the progress of the program. The data includes details such as the number of candidates trained, placed, and the overall performance metrics.

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
