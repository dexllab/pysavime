# PySavime

This code repository (not yet in a mature stage) is an initiative for allowing Python code to directly communicate with
SAVIME server; one can perform queries in and receive data from SAVIME by using its API.
To use this API, you have to perform the following steps:

1. Install the following dependencies: `cython`, `numpy`, `pandas`, 
`sortedcontainers`, and `xarray`; you can do it by creating a new virtual environment based on the
[`requirements.txt`](requirements.txt) file.
2. Install [SAVIME](https://hllustosa.github.io/Savime/) on your machine.
3. Clone this repository. For instance, run `git clone https://github.com/dnasc/pysavime pysavime-src`.
4. If you have changed the default location of SAVIME (while installing it), assign the directory containing the
SAVIME library (`libsavime.a`) to the bash variable `SAVIME_LIB`.
5. In a terminal, run the command `pip install -e  path-to-pysavime-src`: this command will install
`pysavime` in editable mode.

If the compilation ran accordingly, you should be able to use the code in this repository. To test it, 
run `python -c "import pysavime"` in a terminal.

This repository is organized as 
follows:

- `config`: Configurations for global variables.
- `_examples`: Code usage examples.
- `include`: The header for the SAVIME library.
- `logging_utility`: Logging configurations.
- `misc`: Miscellaneous code: decorators, custom exceptions and command runner.
- `savime`: Cython code to communicate with SAVIME.
- `schema`: Set of classes for defining the schema of SAVIME elements: *dataset*, *subtar* and *tar*.
- `util`: Data structures and converters.