from collections import namedtuple, defaultdict, OrderedDict
from collections.abc import Iterable
import numpy as np
from typing import Sequence, Tuple, Union

from savime.datatype import SavimeDataTypeUtility
from misc.decorators import timer_decorator


class DataVariable(namedtuple('DataVariableNamedTuple', ['name', 'array', 'is_dimension'])):
    """
    A SAVIME compatible named array class. It is useful, for example, to store SAVIME query results.

    :param name: Array name.
    :param array: Numpy array.
    :param is_dimension: If true, the data variable stands for dimension values, otherwise data values.

    """

    def __new__(cls, name: str, array: np.array, is_dimension: bool):
        assert SavimeDataTypeUtility.is_array_compatible(array),\
            'The array data d_type must compatible with Savime data types and its rank must be less than or equal to 2.'

        return super().__new__(cls, name, array, is_dimension)

    def __str__(self):
        return f'{self.__class__.__name__}(name={self.name}, array={self.array.__str__()}, is_dimension={self.is_dimension})'


class DataVariableBlock(namedtuple('DataVariableBlockNamedTuple', ['dims', 'attrs'])):
    """
    A SAVIME compatible array block structure. It is useful, for example, to store SAVIME query results.
    """

    def __new__(cls, **kwargs):

        if 'data_variables' in kwargs and len(kwargs) == 1:
            data_variables = kwargs['data_variables']

            if not isinstance(data_variables, Iterable):
                data_variables = (data_variables,)

            cls.__assert_data_variables_validity(data_variables)

            dims, attrs = cls.__process(data_variables)

        elif 'dims' in kwargs and 'attrs' in kwargs and len(kwargs) == 2:
            dims = kwargs['dims']
            attrs = kwargs['attrs']
            cls.__assert_build_data_validity(dims, attrs)

        else:
            raise AttributeError('This class only accepts keyword arguments; in particular, you should provide either '
                                 '`data_variables` or `dims` and `attrs`.')

        obj = super().__new__(cls, dims, attrs)
        return obj

    @staticmethod
    def __process(data_variables: Sequence[DataVariable]) -> Tuple:
        """
        Build the dimension and attribute ordered dictionaries. Each key and value of those dictionaries represents
        respectively a column and its associated array.

        Args:
            data_variables: A sequence of data variables.

        Returns:

        """
        dims = OrderedDict()
        attrs = OrderedDict()

        for data_variable in data_variables:
            if data_variable.is_dimension:
                dims[data_variable.name] = data_variable.array
            else:
                attrs[data_variable.name] = data_variable.array
        return dims, attrs

    @staticmethod
    def __assert_build_data_validity(dims: OrderedDict, attrs: OrderedDict):
        """
        Check if the data (dims and attrs) can be used to build the data variable block.

        Args:
            dims: A dimension dictionary where each key-value pair keeps a named array.
            attrs: An attribute dictionary where each key-value pair keeps a named array.
        """

        assert isinstance(dims, OrderedDict), '`dims` must be an OrderedDict.'

        assert isinstance(attrs, OrderedDict), '`attrs` must be an OrderedDict.'

        def do_values_have_same_length(values):
            return len({len(value) for value in values}) == 1

        assert do_values_have_same_length(dims.values()), 'The shape of dimension arrays must have the same length.'

        assert do_values_have_same_length(attrs.values()), 'The shape of attribute arrays must have the same length.'

    @staticmethod
    def __assert_data_variables_validity(data_variables: Union[Sequence[DataVariable], DataVariable]):
        """
        Check if the data variables can be used to build the data variable block. It is the case only if

        Args:
            data_variables: A sequence of data variables.
        """

        num_elem_first_dim_each_data_variable = set()
        array_names = defaultdict(lambda: 0)

        for data_variable in data_variables:
            assert isinstance(data_variable, DataVariable), f'Each element must be an instance of {DataVariable}.'

            if data_variable.is_dimension:
                assert data_variable.array.shape[-1] == 1, 'Dimension arrays must have rank 1.'

            num_elem_first_dim_each_data_variable.add(data_variable.array.shape[0])
            array_names[data_variable.name] += 1

        assert max(array_names.values()) == 1, 'Data variables must have different names.'
        assert len(num_elem_first_dim_each_data_variable) == 1, 'For each array in a block, the number of elements in ' \
                                                                'the first dimension has to be the same.'

    def __len__(self):
        for elem in self.attrs.values():
            return len(elem)

    def __str__(self):
        return f'{self.__class__.__name__}(dims={self.dims}, attrs={self.attrs})'


class DataVariableBlockOps:

    @classmethod
    @timer_decorator
    def concatenate(cls, data_variable_blocks: Sequence[DataVariableBlock]) -> DataVariableBlock:
        assert len(data_variable_blocks) > 0, 'The sequence of data variable blocks must contain at least one element.'

        cls._validate(data_variable_blocks)

        dim_arrays_concat = defaultdict(lambda: [])
        for data_variable_block in data_variable_blocks:
            for dim_array_name, dim_array in data_variable_block.dims.items():
                dim_arrays_concat[dim_array_name].append(dim_array)

        attr_arrays_concat = defaultdict(lambda: [])
        for data_variable_block in data_variable_blocks:
            for attr_array_name, attr_array in data_variable_block.attrs.items():
                attr_arrays_concat[attr_array_name].append(attr_array)

        dim_arrays_concat = OrderedDict({key: np.concatenate(value, axis=0) for key, value in dim_arrays_concat.items()})
        attr_arrays_concat = OrderedDict({key: np.concatenate(value, axis=0) for key, value in attr_arrays_concat.items()})

        return DataVariableBlock(dims=dim_arrays_concat, attrs=attr_arrays_concat)

    @classmethod
    def _validate(cls, data_variable_blocks):
        def check_keys(elements):
            different = False
            cmp_element = None
            for i, element in enumerate(elements):
                if i == 0:
                    cmp_element = element
                elif element.keys() != cmp_element.keys():
                    different = True
                    break
            return different

        def check_arrays(arrays):
            cmp_array = None
            for i, array in enumerate(arrays):
                if i == 0:
                    cmp_array = array
                else:
                    if array.dtype != cmp_array.dtype:
                        raise AssertionError('Equal attribute/dimension arrays must have the same data d_type. '
                                             f'{array.dtype} != {cmp_array.dtype}')
                    if len(array.shape) != len(cmp_array.shape):
                        raise AssertionError('Equal attribute/dimension arrays must have the same shape. '
                                             f'{len(array.shape)} != {len(cmp_array.shape)}')

        def check_arrays_of_arrays(arrays_of_arrays):
            cmp_array_of_array = None
            for i, array_of_arrays in enumerate(arrays_of_arrays):

                if i == 0:
                    cmp_array_of_array = array_of_arrays
                else:
                    for a1, a2 in zip(cmp_array_of_array, array_of_arrays):
                        check_arrays((a1, a2))

        assert not check_keys(qrb.dims for qrb in data_variable_blocks), 'Data variable blocks must have the same ' \
                                                                          'dimension names (in the same order).'

        check_arrays_of_arrays(qrb.dims.values() for qrb in data_variable_blocks)

        assert not check_keys(qrb.attrs for qrb in data_variable_blocks), 'Data variable blocks must have the same ' \
                                                                           'attributes (in the same order).'

        check_arrays_of_arrays(qrb.attrs.values() for qrb in data_variable_blocks)