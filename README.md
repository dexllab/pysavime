# PySavime

This code repository is an initiative for allowing Python code to directly 
communicate with SAVIME server. The code is far from being
complete and is not yet in a mature stage to be called a
library. Nevertheless, using its API one is able to perform queries
in and receive data from SAVIME. 

To use this API you have to perform the following steps:

1. Install the following dependencies `cython`, `numpy`, `pandas`, 
`sortedcontainers` and `xarray`. This installation can be easily accomplished by using conda
to create a new virtual environment based on `requirements.txt`.
2. Install [SAVIME](https://hllustosa.github.io/Savime/) on your machine.
3. In a terminal, assign the directory containing the SAVIME library (`libsavime.a`)
to the bash variable `SAVIME_LIB` and export it. If you've not changed the default install location while building SAVIME, the library should be in `/usr/local/savime/lib`.
4. In a terminal (in this dir), run the command: `python setup.py build_ext --inplace`.

If the compilation ran accordingly, you should be able to use
the code in this repository. This repository is organized as 
follows:

- `config`: Configurations for global variables.
- `examples`: Code usage examples.
- `include`: The header for SAVIME library.
- `logging_utility`: Logging configurations.
- `misc`: Miscellaneous code: decorators, customs
exceptions and command runner.
- `savime`: Cython code to communicate with SAVIME.
- `schema`: Set of classes for defining the schema of 
SAVIME elements: *dataset*, *subtar* and *tar*.
- `util`: Data structures and converters.
