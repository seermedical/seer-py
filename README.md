# seer-py
Python wrapper for seer-api, with the purpose of authenticating a user, downloading filtered data, and uploading labels.

## Install
To install, simply clone or download this repository, then type `pip install .` which will install all dependencies, and the Seer python API.

## Requirements
This library currently requires python 3, and it if you don't currently have a python 3 installation, we recommend you use the Anaconda distribution for its simplicity, support, stability and extensibility. It can be downloaded here: https://www.anaconda.com/download

GraphQL python library is required to query the Seer database (in addition to Anaconda). To install, simply run:
`pip install gql`

Pandas, numpy, and matplotlib are also required. Some of these installs can be tricky, so Anaconda is recommended, however, they can be installed separately. Please see these guides for more detailed information:
https://scipy.org/install.html
https://matplotlib.org/users/installing.html
https://pandas.pydata.org/pandas-docs/stable/install.html

To run the jupyter notebook example (optional, included in Anaconda):
`pip install notebook`

## Getting Started

Check out the [Example](Examples/Example.ipynb) for a step-by-step example of how to use the API

To start jupyter notebooks, run `jupyter notebook` in a command/bash window. Further instructions on Jupyter can be found here: https://github.com/jupyter/notebook
