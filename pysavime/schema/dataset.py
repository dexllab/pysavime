from pysavime.savime.datatype import SavimeSupportedTypes
from pysavime.schema.schema import CreatableSavimeElement, DroppableSavimeElement, Literal, Range


class Dataset(CreatableSavimeElement, DroppableSavimeElement):
    """
    Defines an abstract interface for a dataset. In Savime a dataset is a contiguous array of primitive data type
    elements. In particular, Savime supports the definition of one and two rank arrays. Only the children of this
    class should be instantiated.

    Attributes:
        name: The dataset name.
        data_type: The dataset data type.
        num_columns: The dimension (number of columns) the array in this dataset has.

    """

    def __init__(self, name: str, data_type: SavimeSupportedTypes, num_columns: int = 1):
        self.name = name
        self.data_type = data_type
        self.num_columns = num_columns

        assert num_columns > 0

    def plain_query_str(self):
        q = f'"{self.name}:{self.data_type.value}:{self.num_columns}"' + ', {}'
        return q

    def create_query_str(self):
        q = f'CREATE_DATASET({self.plain_query_str()});'
        return q

    def drop_query_str(self):
        q = f'DROP_DATASET("{self.name}");'
        return q


class FileDataset(Dataset):
    """
    Defines a file dataset, i.e., stands for an array stored locally.

    Examples:
        >>> FileDataset(name='dataset', data_type=SavimeSupportedTypes.INT32, dimension=2, file_path='/home/user/array',
        is_in_savime_storage=False)

    Attributes:
        file_path: The path where the file is located.
        is_in_savime_storage: If False the file will be copied to the Savime storage, usually a memory mapped location.
    """

    def __init__(self, file_path: str, is_in_savime_storage: bool, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.is_in_savime_storage = is_in_savime_storage

    def plain_query_str(self):
        class_specific_str = f'"{"" if self.is_in_savime_storage else "@"}{self.file_path}"'
        q = super().plain_query_str().format(class_specific_str)
        return q


class LiteralDataset(Dataset, CreatableSavimeElement):
    """
    Defines a literal dataset.

    Examples:
        >>> LiteralDataset(name='dataset', literal=Literal([1, 2, 3]))

    Attributes:
        name: A dataset name.
        literal: A literal.
    """

    def __init__(self, name: str, literal: Literal, num_columns: int = 1):
        super().__init__(name=name, data_type=literal.data_type, num_columns=num_columns)
        self.literal = literal

        assert self.data_type == literal.data_type

    def plain_query_str(self):
        class_specific_str = f'{self.literal.plain_query_str()}'
        q = super().plain_query_str().format(class_specific_str)
        return q


class RangeDataset(Dataset, CreatableSavimeElement):
    """
    Defines a range dataset.

    Examples:
        >>> RangeDataset('dataset', Range(1, 5, 1, 2), SavimeSupportedTypes.UINT32)

    Attributes:
        name: A dataset name.
        range: A dataset range.
        data_type: A dataset data type.
    """

    def __init__(self, name: str, range: Range, data_type: SavimeSupportedTypes):
        super().__init__(name=name, data_type=data_type, num_columns=1)
        self.range = range

    def plain_query_str(self):
        class_specific_str = f'{self.range.plain_query_str()}'
        q = super().plain_query_str().format(class_specific_str)
        return q
