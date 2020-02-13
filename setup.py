import os

from setuptools import setup
from setuptools.extension import Extension
from Cython.Build import cythonize

SAVIME_INCLUDE_ENV = 'SAVIME_INCLUDE'
SAVIME_LIB_ENV = 'SAVIME_LIB'

DEFAULT_SAVIME_LIB = '/usr/local/savime/lib'
DEFAULT_SAVIME_INCLUDE = 'include/'

SAVIME_INCLUDE = None
SAVIME_LIB = None


def check_savime_install():
    global SAVIME_INCLUDE, SAVIME_INCLUDE_ENV, SAVIME_LIB, SAVIME_LIB_ENV, DEFAULT_SAVIME_INCLUDE, DEFAULT_SAVIME_LIB

    def assign_value(variable, default_value, environment_variable):
        if os.environ.get(environment_variable) is not None:
            variable = os.environ.get(environment_variable)

        elif os.path.exists(default_value):
            variable = default_value

        type_ = 'include' if 'include' in environment_variable.lower() else 'lib'

        if variable is None:
            raise Exception(f'You should set the {type_} directory through the environment variable {environment_variable}.')

        return variable

    SAVIME_INCLUDE = assign_value(SAVIME_INCLUDE, DEFAULT_SAVIME_INCLUDE, SAVIME_INCLUDE_ENV)
    SAVIME_LIB = assign_value(SAVIME_LIB, DEFAULT_SAVIME_LIB, SAVIME_LIB_ENV)


check_savime_install()


extensions = [
    Extension('client', ['savime/client.pyx'],
              include_dirs=[SAVIME_INCLUDE],
              libraries=['savime'],
              library_dirs=[SAVIME_LIB],
              language='c++'),
    Extension('datatype', ['savime/datatype.pyx'],
              include_dirs=[SAVIME_INCLUDE],
              libraries=['savime'],
              library_dirs=[SAVIME_LIB],
              language='c++'),
]

setup(ext_modules=cythonize(extensions,
                            compiler_directives={'language_level': "3"},
                            build_dir='build'))


def find_lib_and_move_to_dir(start_strings, dir_path):
    for elem in os.listdir('.'):
        for start_string in start_strings:
            if os.path.isfile(elem) and elem.startswith(start_string):
                os.rename(elem, os.path.join(dir_path, elem))


find_lib_and_move_to_dir([extension.name for extension in extensions], 'savime')
