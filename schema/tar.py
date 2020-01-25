from abc import abstractmethod
from typing import Sequence, Union

from savime.datatype import SavimeSupportedTypes
from schema.schema import CreatableSavimeElement, IntervalRange, SavimeElement


class TarMetaType(CreatableSavimeElement):
    """
    Defines a meta type for tars by naming dimensions and attributes.

    Examples:
        >>> TarMetaType('weather_forecasting_data_type', ['latitude', 'longitude', 'time_step'], \
        >>> ['pressure', 'air', 'temperature', 'precipitation'])

    Attributes:
        name: The name of the abstract type.
        dimension_names: The name of each dimension.
        attribute_names: The name of each attribute.
    """

    def __init__(self, name: str, dimension_names: Sequence[str], attribute_names: Sequence[str]):
        self.name = name
        self.dimension_names = dimension_names
        self.attribute_names = attribute_names

    def plain_query_str(self):
        dimension_names_str = ', '.join(self.dimension_names)
        attribute_names_str = ', '.join(self.attribute_names)
        q = f'{self.name}({dimension_names_str}, {attribute_names_str})'
        return q

    def create_query_str(self):
        q = f'CREATE_TYPE("{self.plain_query_str()}");'
        return q


class TarAttributeSpecification(SavimeElement):
    """
    Defines tar attribute specifications.

    Examples:
        >>> # Defines a float attribute named 'a' with shape [None, 2].
        >>> TarAttributeSpecification('a', SavimeSupportedTypes.FLOAT, 2)

    Attributes:
        name: The attribute name.
        data_type: The attribute data_type.
        num_columns: The attribute number of columns.
    """

    def __init__(self, name: str, data_type: SavimeSupportedTypes, num_columns: int = 1):
        self.name = name
        self.data_type = data_type
        self.num_columns = num_columns

        assert self.num_columns > 0

    def plain_query_str(self) -> str:
        q = f'{self.name}, {self.data_type.value}: {self.num_columns}'
        return q


class TarDimensionSpecification(SavimeElement):
    """
    Defines tar dimension specifications.

    Attributes:
        name: The dimension name.
    """

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def plain_query_str(self) -> str:
        pass


class ImplicitTarDimensionSpecification(TarDimensionSpecification):
    """
    Defines a implicit tar dimension.

    Examples:
        >>> # A dimension named 'latitude' valued between 1 and 10.
        >>> ImplicitTarDimensionSpecification('latitude', SavimeSupportedTypes.INT32, IntervalRange(1, 10))

    Attributes:
        name: The dimension name.
        data_type: the dimension data type.
        interval: An interval which implicitly defines the dimension.
    """

    def __init__(self, name: str, data_type: SavimeSupportedTypes, interval: IntervalRange):
        super().__init__(name)
        self.data_type = data_type
        self.interval = interval

    def plain_query_str(self) -> str:
        q = f'implicit, {self.name}, {self.data_type.value}, {self.interval.start}, {self.interval.stop},' \
            f' {self.interval.step}'
        return q


class ExplicitTarDimensionSpecification(TarDimensionSpecification):
    """
    Defines a explicit tar dimension, i.e., by a dataset.

    Examples:
        >>> ExplicitTarDimensionSpecification('latitude','latitude_array')

    Attributes:
        name: The dimension name.
        dataset: A dataset or its name.
    """

    def __init__(self, name: str, dataset: str):
        super().__init__(name)
        self.dataset = dataset
        self._dataset_name = dataset if isinstance(dataset, str) else dataset.name

    def plain_query_str(self) -> str:
        q = f'explicit, {self.name}, {self._dataset_name}'
        return q


class Tar(CreatableSavimeElement):
    """
    Defines a tar.

    Examples:
        >>> weather_phenomena_type = TarMetaType('weather_phenomena', ['latitude', 'longitude', 'time'],
        ['temperature','precipitation'])
        >>> latitude = ImplicitTarDimensionSpecification('lat', SavimeSupportedTypes.UINT32, IntervalRange(1, 180))
        >>> longitude = ImplicitTarDimensionSpecification('lon', SavimeSupportedTypes.UINT32, IntervalRange(1, 360))
        >>> time = ImplicitTarDimensionSpecification('t', SavimeSupportedTypes.UINT32, IntervalRange(1, 1000))
        >>> temperature = TarAttributeSpecification('temperature', SavimeSupportedTypes.DOUBLE)
        >>> precipitation = TarAttributeSpecification('precipitation', SavimeSupportedTypes.DOUBLE)
        >>> mapping = {latitude.name: 'latitude', longitude.name: 'longitude', time.name: 'time'}
        >>> weather_schema = Tar('weather_phenomena_tar', [latitude, longitude, time], [temperature, precipitation],
        weather_phenomena_type, mapping)

    Attributes:
        name: The tar name.
        dimension_specification: A sequence of dimension specifications.
        attribute_specification: A sequence of attribute specifications.
        meta_type: A meta type.
        meta_type_mapping: A mapping between the meta type and the attributes/dimensions.
        in the dimension specification. The mapping has to have the form {dimension/attribute name: meta_type value}.
    """

    def __init__(self, name: str,
                 dimension_specification: Sequence[Union[ImplicitTarDimensionSpecification,
                                                         ExplicitTarDimensionSpecification]],
                 attribute_specification: Sequence[TarAttributeSpecification],
                 meta_type: Union[str, TarMetaType] = None,
                 meta_type_mapping: dict = None):

        self.name = name
        self.dimension_specification = dimension_specification
        self.attribute_specification = attribute_specification
        self.meta_type = meta_type
        self.meta_type_mapping = meta_type_mapping

        assert not ((self.meta_type is not None) ^ (self.meta_type_mapping is not None)), \
            'If you provide a meta type for the tar, you also have to provide a  mapping between the ' \
            'attributes/dimensions and the type.'

    def plain_query_str(self) -> str:
        q = f'"{self.name}", "{self.meta_type_query_str}", "{self.dimension_specification_str}", ' \
            f'"{self.attribute_specification_str}" {self.mapping_query_str} '
        return q

    @property
    def dimension_specification_str(self) -> str:
        return ' | '.join(dimension.plain_query_str() for dimension in self.dimension_specification)

    @property
    def attribute_specification_str(self) -> str:
        return ' | '.join(attribute.plain_query_str() for attribute in self.attribute_specification)

    @property
    def meta_type_query_str(self) -> str:
        if self.meta_type is not None:
            return self.meta_type.name if isinstance(self.meta_type, TarMetaType) else self.meta_type
        return '*'

    @property
    def mapping_query_str(self) -> str:
        if self.meta_type is not None:
            return ' ,"' + ', '.join(f'{key}, {value}' for key, value in self.meta_type_mapping.items()) + '"'
        return ''

    def create_query_str(self) -> str:
        q = f'CREATE_TAR({self.plain_query_str()});'
        return q
