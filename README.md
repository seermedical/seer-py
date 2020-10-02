# seer-py

Python SDK for the Seer data platform, which handles authenticating a user, downloading channel data, and uploading labels/annotations.

## Access to data
Note that by default there is no public access to any data on the Seer Cloud. You need to be added to an organisation or user group within the Seer Cloud in order to gain access to data. The only publicly accessible data is through the https://www.epilepsyecosystem.org/ user group.

## Install

To install, simply clone or download this repository, then type `pip install .` which will install all the dependencies.

### Epilepsy Ecosystem Data

For users attempting to download data for the [Epilepsy Ecosystem](https://www.epilepsyecosystem.org/howitworks/), please download the [latest master version](https://github.com/seermedical/seer-py/tree/master).

#### My Seizure Gauge Data

For the My Seizure Gauge dataset, use the script `msg_data_downloader.py` in `Examples` to begin the download process. To specify a save-path for the downloaded data, use the command `python msg_data_downloader.py -o /path/to/directory` where `/path/to/directory` is the desired save path. If download is aborted, this script can be re-run and the download should resume from where it was stopped.

#### NeuroVista Data

For the NeuroVista dataset, open the script `neurovista_contest_data_downloader.py.py` in `Examples` and it will guide you through the download process (you will need to change a few things in this script including the path to download the data to).

## Requirements

This library currently requires Python 3, and it if you don't currently have a Python 3 installation, we recommend you use the Anaconda distribution for its simplicity, support, stability and extensibility. It can be downloaded here: https://www.anaconda.com/download

The install instructions above will install all the required dependencies, however, if you wish to install them yourself, here's what you'll need:

- [`gql`](https://github.com/graphql-python/gql): a GraphQL python library is required to query the Seer platform. To install, simply run: `pip install gql`
- Pandas, numpy, and matplotlib are also required. Some of these installs can be tricky, so Anaconda is recommended, however, they can be installed separately. Please see these guides for more detailed information:
  - https://scipy.org/install.html
  - https://matplotlib.org/users/installing.html
  - https://pandas.pydata.org/pandas-docs/stable/install.html

To run the Jupyter notebook example (optional, included in Anaconda): `pip install notebook`

## Getting Started

Check out the [Example](Examples/Example.ipynb) for a step-by-step example of how to use the SDK to access data on the Seer platform.

To start a Jupyter notebook, run `jupyter notebook` in a command/bash window. Further instructions on Jupyter can be found here: https://github.com/jupyter/notebook

## Authenticating

To access data from the Seer platform you must first register for an account at https://app.seermedical.com/. You can then use seer-py to authenticate with the API.

Depending on which Seer server you are accessing, there are 2 different ways to authenticate.
1. Using email and password (for https://api.seermedical.com/api)
2. Using an access key (for https://sdk-au.seermedical.com/api and https://sdk-uk.seermedical.com/api)

### Using email and password

seer-py will prompt for the username and password that you use to log in to the Seer App. The simplest way to authenticate is to create an instance of `SeerConnect`:

```
from seerpy import SeerConnect

client = SeerConnect()
```

Alternatively, you can provide your `email` and `password` as arguments to `SeerConnect`

```
client = SeerConnect(email='user@website.com', password='....')
```

You can also store your credentials by creating a _.seerpy/_ folder in your home directory. Within the folder, create a file named _credentials_. Save your email address on the first line and your password on the second line with no other text. You can then call SeerConnect as above:

```
client = SeerConnect()
```

Note: if you have an api key file in _.seerpy/_ but wish to use email authentication, pass the `use_email=True` flag to `SeerConnect`

```
client = SeerConnect(use_email=True)
```

### Using an api key

To access the SDK servers, you will need to ask for an API key to be generated for you.

An API key consists of a key file and an id. You can supply these when constructing `SeerConnect`
```
client = SeerConnect(api_key_id='id', api_key_path='~/.seerpy/seerpy.pem')
```

Alternatively, seerpy will look for an api key file in the _.seerpy/_ folder in your home directory.
It will look for any file like _seerpy.pem_
You can also encode the id and region in the file name like so _seerpy.id.au.pem_

It's best to store the key in a file, but in situations where this is difficult or impossible, you can instead create an instance of `seerpy.auth.SeerApiKeyAuth` and pass the string api_key directly, i.e:
```
seer_auth = auth.SeerApiKeyAuth(api_key_id='id', api_key='private key string')
client = SeerConnect(seer_auth=seer_auth)
```
In this implementation, `'private key string'`, should be exactly the key string returned from the create API key mutation (without replacing literal `\n` characters). However, in some environments the `\n` characters may need to be replaced with line feeds. It is recommended to use the exact key string first, and if a key error is recieved (`could not deserialize key data`) then try replacing `\n` with line feeds.   

## Running with other API endpoints

To run seer-py in other environments (for instance against a development API server), then you can use one of the preconfigured authentication methods:

```
from seerpy import auth, SeerConnect
client = SeerConnect(auth=auth.SeerDevAuth('http://localhost:8000'))
```

When running an API server locally, you may also need to:

1. Run the `seer-api` server following the relevant documentation in the API repository
2. Run a `seer-graphiql` server (which includes the required proxies), ensuring that `HTTPS` is set to `OFF` in the startup script

## Troubleshooting

### Downloading hangs on Windows

There is a known issue with using python's multiprocessing module on Windows with spyder. The function `get_channel_data` uses `multiprocessing.Pool` to run multiple downloads simultaneously, which can cause the process to run indefinitely. The workaround for this is to ensure that the current working directory is set to the directory containing your script. Running the script from a command window will also solve this problem. Alternatively, setting `threads=1` in the `get_channel_data` function will stop in from using `multiprocessing` altogether.


## Development

1. To format the code using yapf, run `yapf -ir seerpy tests`
2. To run pylint on the code, run `pylint seerpy tests`
3. To run tests and generate an html coverage report, run `pytest --cov-report=html --cov=seerpy`
