rbi_atm_pos
==============================

This project involves collecting and processing ATM/POS/Card statistics from the Reserve Bank of India (RBI) website. The data provides insights into the distribution and usage of ATMs, POS (Point of Sale) machines, and cards across different banks.The data is sourced from the RBI official website: https://rbi.org.in/Scripts/ATMView.aspx. The website contains valuable information on ATM, POS, and card statistics for various banks.

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