from pysavime.savime.savime cimport SavimeEnumType
from enum import Enum
from collections import namedtuple
import numpy as np


class SavimeSupportedTypes(Enum):
    CHAR = 'char'
    INT8 = 'int8'
    INT16 = 'int16'
    INT32 = 'int32'
    INT64 = 'int64'
    UINT8 = 'uint8'
    UINT16 = 'uint16'
    UINT32 = 'uint32'
    UINT64 = 'uint64'
    FLOAT = 'float'
    DOUBLE = 'double'

    def __repr__(self):
        return f'{self.__class__.__name__}.{self._name_}'


class SavimeDataTypeUtility:
    __mapper_type = namedtuple('MapperType', ['savime', 'numpy', 'query', 'size'])

    __mapper = (
    __mapper_type(SavimeEnumType.SAV_CHAR, np.int8, SavimeSupportedTypes.CHAR, np.dtype(np.int8).itemsize, ),
    __mapper_type(SavimeEnumType.SAV_INT8, np.int8, SavimeSupportedTypes.INT8, np.dtype(np.int8).itemsize),
    __mapper_type(SavimeEnumType.SAV_INT16, np.int16, SavimeSupportedTypes.INT16, np.dtype(np.int16).itemsize),
    __mapper_type(SavimeEnumType.SAV_INT32, np.int32, SavimeSupportedTypes.INT32, np.dtype(np.int32).itemsize),
    __mapper_type(SavimeEnumType.SAV_INT64, np.int64, SavimeSupportedTypes.INT64, np.dtype(np.int64).itemsize),
    __mapper_type(SavimeEnumType.SAV_UINT8, np.uint8, SavimeSupportedTypes.UINT8, np.dtype(np.uint8).itemsize),
    __mapper_type(SavimeEnumType.SAV_UINT16, np.uint16, SavimeSupportedTypes.UINT16, np.dtype(np.uint16).itemsize),
    __mapper_type(SavimeEnumType.SAV_UINT32, np.uint32, SavimeSupportedTypes.UINT32, np.dtype(np.uint32).itemsize),
    __mapper_type(SavimeEnumType.SAV_UINT64, np.uint64, SavimeSupportedTypes.UINT64, np.dtype(np.uint64).itemsize),
    __mapper_type(SavimeEnumType.SAV_FLOAT, np.float32, SavimeSupportedTypes.FLOAT, np.dtype(np.float32).itemsize),
    __mapper_type(SavimeEnumType.SAV_DOUBLE, np.float64, SavimeSupportedTypes.DOUBLE, np.dtype(np.float64).itemsize),
    __mapper_type(SavimeEnumType.INVALID_TYPE, None, '', 0))

    @classmethod
    def __find(cls, key, source_d_type, target_d_type):
        for elem in cls.__mapper:
            if elem.__getattribute__(source_d_type) == key:
                return elem.__getattribute__(target_d_type)

    @classmethod
    def savime_primitive_to_numpy(cls, key):
        return cls.__find(key, 'savime', 'numpy')

    @classmethod
    def savime_to_size(cls, key):
        return cls.__find(key, 'savime', 'size')

    @classmethod
    def savime_primitive_to_savime_query(cls, key):
        return cls.__find(key, 'savime', 'query')

    @classmethod
    def numpy_to_savime_primitive(cls, key):
        return cls.__find(key, 'numpy', 'savime')

    @classmethod
    def numpy_to_savime_query(cls, key):
        return cls.__find(key, 'numpy', 'query')

    @classmethod
    def numpy_to_size(cls, key):
        return cls.__find(key, 'numpy', 'size')

    @classmethod
    def get_size(cls, key, size_t num_elements=1, size_t length=1, type_='savime'):
        if type_ == 'savime':
            return cls.savime_to_size(key) * num_elements * length
        elif type_ == 'numpy':
            return cls.savime_to_size(key) * num_elements * length

    @classmethod
    def is_numpy_dtype_compatible(cls, numpy_dtype):
        return cls.numpy_to_savime_primitive(numpy_dtype) is not None or numpy_dtype.char == 'U'

    @classmethod
    def is_array_compatible(cls, array: np.array):
        return SavimeDataTypeUtility.is_numpy_dtype_compatible(array.dtype) and len(array.shape) <= 2
