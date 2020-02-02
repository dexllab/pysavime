from abc import ABC
from collections import OrderedDict
import itertools
import numpy as np
import os
import pandas as pd
from sortedcontainers import SortedDict
import xarray as xr

from util.data_variable import DataVariableBlock
from misc.decorators import timer_decorator

from schema.tar import *
from schema.dataset import *
from savime.datatype import SavimeDataTypeUtility
from savime.client import Client
from config import *


# TODO: Strings
def numpy_array_to_savime_dataset(array: np.array, client: Client, dataset_name: str,
                                  dataset_dir_path: str = DEFAULT_SAVIME_STORAGE_DIR_PATH,
                                  savime_storage_dir_path: str = DEFAULT_SAVIME_STORAGE_DIR_PATH):

    assert SavimeDataTypeUtility.is_array_compatible(array)
    dataset_file_path = os.path.join(dataset_dir_path, dataset_name)
    array.tofile(dataset_file_path)

    is_in_savime_storage = dataset_dir_path.startswith(savime_storage_dir_path)
    assert len(array.shape) <= 2
    num_columns = 1 if len(array.shape) else array.shape[-1]
    dataset = FileDataset(name=dataset_name, data_type=SavimeDataTypeUtility.numpy_to_savime_query(array.dtype),
                          file_path=dataset_file_path, is_in_savime_storage=is_in_savime_storage, dimension=num_columns)
    client.create(dataset)

    return dataset


def load_sub_tar(dims):
    pass


'''
def data_variable_block_to_tar(data_variable_block: DataVariableBlock, client: Client, dataset_prefix: str = '',
                               datasets_dir_path: str = DEFAULT_SAVIME_STORAGE_DIR_PATH,
                               savime_storage_dir_path: str = DEFAULT_SAVIME_STORAGE_DIR_PATH):

    def create_dataset_name(array_name):
        return f'{dataset_prefix}_{array_name}' if dataset_prefix != '' else array_name

    dim_name_to_dataset = {}

    for dim_name, dim_array in data_variable_block.dims.items():
        dataset_name = create_dataset_name(dim_name)
        numpy_array_to_savime_dataset(dim_array, client, dataset_name, datasets_dir_path, savime_storage_dir_path)
        dim_name_to_dataset[dim_name] = dataset_name

    for dim_name, dataset in dim_name_to_dataset.items():
        array_length = len(data_variable_block.dims[dim_name])
        SpecifiedByDatasetSubTarDimensionSpecification(dim_name, 0, array_length, False, dataset, 'total')
'''


class DataVariableBlockConverterABC(ABC):

    def __new__(cls, data_variable_block: DataVariableBlock):
        return super().__new__(cls).__call__(data_variable_block)

    @abstractmethod
    def __call__(self, data_variable_block: DataVariableBlock):
        pass


class PandasDataFrameToDataVariableBlock:

    def __call__(self, data_frame: pd.DataFrame) -> DataVariableBlock:
        dims = self.index_to_dims(data_frame.index)
        attrs = self.columns_to_attrs(data_frame)

        return DataVariableBlock(dims=dims, attrs=attrs)

    @classmethod
    def columns_to_attrs(cls, data_frame: pd.DataFrame) -> OrderedDict:
        attrs = OrderedDict()
        is_column_index_multi_index = cls.is_index_multi_index(data_frame.columns)
        for column_index in data_frame.columns:
            column_name = column_index[0] if is_column_index_multi_index else column_index
            if column_name not in attrs:
                attrs[column_name] = data_frame[column_name].values
        return attrs

    @classmethod
    def index_to_dims(cls, index: Union[pd.Index, pd.MultiIndex]) -> OrderedDict:
        if cls.is_index_multi_index(index):
            return cls.multi_index_to_dim(index)
        else:
            return cls.index_to_dims(index)

    @staticmethod
    def simple_index_to_dim(index: pd.Index) -> OrderedDict:
        assert index.name is not None
        dims = OrderedDict()
        dims[index.name] = index.values
        return dims

    @staticmethod
    def multi_index_to_dim(multi_index: pd.MultiIndex) -> OrderedDict:
        dims = OrderedDict()
        for name, array in zip(multi_index.names, multi_index.codes):
            assert name is not None
            dims[name] = array.values()
        return dims

    @staticmethod
    def is_index_multi_index(index: Union[pd.Index, pd.MultiIndex]) -> bool:
        return isinstance(index, pd.MultiIndex)


class DataVariableBlockToPandasDataFrame(DataVariableBlockConverterABC):

    @timer_decorator
    def __call__(self, data_variable_block: DataVariableBlock) -> pd.DataFrame:

        dim_names = [key for key in data_variable_block.dims.keys()]
        dim_columns = [dim.ravel() for dim in data_variable_block.dims.values()]

        attr_names = []
        attr_columns = []
        attr_columns_multi_index = False

        for attr_name, attr_array in data_variable_block.attrs.items():
            num_columns = 1 if len(attr_array.shape) == 1 else attr_array.shape[-1]
            if num_columns == 1:
                attr_array.shape = (attr_array.shape[0], 1)
                attr_columns.append(attr_array)
                attr_names.append(attr_name)
            else:
                attr_columns_multi_index = True
                attr_columns.append(attr_array)
                num_columns = attr_array.shape[-1]
                attr_name_with_column_indices = list(zip(itertools.repeat(attr_name, num_columns), range(num_columns)))
                attr_names.extend(attr_name_with_column_indices)

        dim_columns = self.dim_columns_to_index(dim_names, dim_columns)
        attr_names = self.attr_names_to_index(attr_names, attr_columns_multi_index)
        data_frame = pd.DataFrame(np.hstack(attr_columns), index=dim_columns, columns=attr_names)

        return data_frame

    @staticmethod
    def dim_columns_to_index(dim_names, dim_columns):
        if len(dim_names) == 1:
            return pd.Index(dim_columns[0], name=dim_names[0])
        else:
            return pd.MultiIndex.from_arrays(dim_columns, names=dim_names)

    @staticmethod
    def attr_names_to_index(attr_names, multi_index):
        if not multi_index:
            return pd.Index(attr_names)
        else:
            return pd.MultiIndex.from_tuples(attr_name if isinstance(attr_name, tuple) else (attr_name, 0)
                                             for attr_name in attr_names)


class DataVariableBlockToXarrayDataset(DataVariableBlockConverterABC):

    @timer_decorator
    def __call__(self, data_variable_block: DataVariableBlock) -> xr.Dataset:

        def is_a_matrix(array):
            return len(array.shape) != 1 and (len(array.shape) != 2 or array.shape[1] != 1)

        def tile_arrays(arrays, num_times):
            return [np.tile(array, num_times) for array in arrays]

        def repeat_arrays(arrays, num_times):
            return [np.repeat(array, num_times) for array in arrays]

        shape = [len(np.unique(dim_array)) for dim_array in data_variable_block.dims.values()]
        coord_and_index_array_names = [dim_array_name for dim_array_name in data_variable_block.dims.keys()]
        index_arrays = [dim_array.ravel() for dim_array in data_variable_block.dims.values()]
        mapped_index_arrays = self.__mapped_indices(index_arrays)
        coord_arrays = [np.unique(dim_array.ravel()) for dim_array in data_variable_block.dims.values()]

        xr_arrays = {}

        for attr_name, attr_array in data_variable_block.attrs.items():
            attr_mapped_index_arrays = mapped_index_arrays
            attr_dense_shape = shape[:]
            attr_index_arrays = index_arrays[:]
            attr_coord_and_index_array_names = coord_and_index_array_names[:]
            attr_coord_arrays = coord_arrays[:]

            if is_a_matrix(attr_array):
                attr_dense_shape += [attr_array.shape[1]]
                # Tile dimension indices and repeat column indices.
                attr_index_arrays = tile_arrays(attr_index_arrays, attr_array.shape[1]) + \
                                    repeat_arrays([np.arange(attr_array.shape[1])], attr_array.shape[0])
                # Add one coord for matrix columns
                attr_coord_and_index_array_names += ['_0_']
                attr_coord_arrays += [np.arange(attr_array.shape[1], dtype=np.int)]

                # Create map for column indices
                attr_mapped_index_arrays = self.__mapped_indices(attr_index_arrays)

                # Create flatten representation for attribute array
                attr_array = np.concatenate(np.hsplit(attr_array, attr_array.shape[1]), axis=0)

            attr_array = attr_array.ravel()
            out_array = np.empty(attr_dense_shape, dtype=attr_array.dtype)
            out_array[tuple(attr_mapped_index_arrays)] = attr_array

            mask = np.zeros(attr_dense_shape, dtype=bool)
            mask[tuple(attr_mapped_index_arrays)] = True

            d = dict(zip(attr_coord_and_index_array_names, attr_coord_arrays))
            d['_mask_'] = (tuple(attr_coord_and_index_array_names), mask)

            xr_array = xr.DataArray(out_array, coords=d, dims=attr_coord_and_index_array_names)
            xr_arrays[attr_name] = xr_array

        return xr.Dataset(xr_arrays)

    @staticmethod
    def __mapped_indices(dims):
        mapper = []
        for dim_array in dims:
            unique = np.unique(dim_array)
            unique_range = range(len(unique))
            dim_array_mapper = SortedDict(zip(unique, unique_range))
            mapper.append(dim_array_mapper)

        indices = []
        for i, dim_array in enumerate(dims):
            dim_array_indices = np.array([mapper[i][dim_index] for dim_index in dim_array.ravel()])
            indices.append(dim_array_indices)

        return tuple(indices)


class DataVariableBlockConverter:
    __slots__ = ('target_type', '__chosen_converter')
    __available_converters = {'pandas': DataVariableBlockToPandasDataFrame,
                              'xarray': DataVariableBlockToXarrayDataset}

    def __init__(self, target_type):
        self.target_type = target_type
        self.__set_converter()

    def __call__(self, data_variable_block: DataVariableBlock):
        return self.__chosen_converter(data_variable_block)

    def __set_converter(self):
        try:
            self.__chosen_converter = self.__available_converters[self.target_type]
        except KeyError:
            raise
