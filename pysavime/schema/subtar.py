from abc import abstractmethod
from typing import Sequence, Union

from pysavime.savime.datatype import SavimeSupportedTypes
from pysavime.schema.dataset import Dataset
from pysavime.schema.schema import IndexRange, IntervalRange, LoadableSavimeElement, SavimeElement
from pysavime.schema.tar import Tar, TarAttributeSpecification, ImplicitTarDimensionSpecification, \
    TarDimensionSpecification


class SubTarDimensionSpecification(SavimeElement):
    """
    Defines a dimension for a subtar.

    Attributes:
        dimension: The target dimension specification or its name.
        index_range: The index range for the slice.
    """

    def __init__(self, dimension: Union[str, TarDimensionSpecification], index_range: IndexRange):
        self.dimension = dimension
        self.index_range = index_range
        self.index_prefix = '' if self.index_range.is_physical else '#'
        self._dimension_name = dimension if isinstance(dimension, str) else dimension.name

    @abstractmethod
    def plain_query_str(self) -> str:
        pass


class OrderedSubTarDimensionSpecification(SubTarDimensionSpecification):
    """
    Defines a subtar ordered dimension.

    Examples:
        >>> # Defines a ordered slice of latitude from 1 to 90.
        >>> latitude = ImplicitTarDimensionSpecification('lat', SavimeSupportedTypes.UINT32, IntervalRange(1, 180))
        >>> OrderedSubTarDimensionSpecification(latitude.name, IndexRange(1, 90, False))
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def plain_query_str(self) -> str:
        q = f'ordered, {self._dimension_name}, {self.index_prefix}{self.index_range.start},' \
            f'{self.index_prefix}{self.index_range.stop}'
        return q


class SpecifiedByDatasetSubTarDimensionSpecification(SubTarDimensionSpecification):
    """
    Defines a subtar dimension which is specified by a dataset.

    Attributes:
        dataset: A dataset or its name.
    """

    _type_placeholder_str = '__type__'

    def __init__(self, dataset: Union[str, Dataset], **kwargs):
        super().__init__(**kwargs)
        self.dataset = dataset
        self._dataset_name = self.dataset.name if isinstance(self.dataset, Dataset) else self.dataset

    def plain_query_str(self) -> str:
        placeholder = '{' + self._type_placeholder_str + '}'
        q = placeholder + f', {self._dimension_name}, {self.index_prefix}{self.index_range.start}, ' \
                          f'{self.index_prefix}{self.index_range.stop}, {self._dataset_name}'
        return q


class PartialSubTarDimensionSpecification(SpecifiedByDatasetSubTarDimensionSpecification):
    """
    Defines a subtar partial dimension.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def plain_query_str(self) -> str:
        return super().plain_query_str().format(**{self._type_placeholder_str: 'partial'})


class TotalSubTarDimensionSpecification(SpecifiedByDatasetSubTarDimensionSpecification):
    """
    Defines a subtar total dimension.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def plain_query_str(self) -> str:
        return super().plain_query_str().format(**{self._type_placeholder_str: 'total'})


class SubTarAttributeSpecification(SavimeElement):
    """
    Defines a sutar attribute.

    Examples:
        Defines that a dataset named temperature_array will be loaded in the subtar.
        >>> temperature = TarAttributeSpecification('temperature', SavimeSupportedTypes.FLOAT)
        >>> SubTarAttributeSpecification(attribute_specification=temperature, dataset_name='temperature_array')

    Attributes:
        attribute: An attribute name or specification.
        dataset: A dataset or its name.

    """

    def __init__(self, attribute: Union[TarAttributeSpecification, str], dataset: Union[str, Dataset]):
        self.attribute_specification = attribute
        self.dataset = dataset
        self._dataset_name = dataset if isinstance(dataset, str) else dataset.name
        self._attribute_specification_name = attribute if isinstance(attribute, str) \
            else attribute.name

    def plain_query_str(self) -> str:
        q = f'{self._attribute_specification_name}, {self._dataset_name}'
        return q


class SubTar(LoadableSavimeElement):
    """
    Defines a subtar.
    """

    def __init__(self, tar: Union[str, Tar],
                 dimension_specification: Sequence[SubTarDimensionSpecification],
                 attribute_specification: Sequence[SubTarAttributeSpecification]):
        self.tar = tar
        self.dimension_specification = dimension_specification
        self.attribute_specification = attribute_specification

    def plain_query_str(self) -> str:
        tar_name = self.tar.name if isinstance(self.tar, Tar) else self.tar
        attribute_specification_str = ' | '.join(
            attribute.plain_query_str() for attribute in self.attribute_specification)
        dimension_specification_str = ' | '.join(
            dimension.plain_query_str() for dimension in self.dimension_specification)
        q = f'"{tar_name}", "{dimension_specification_str}", "{attribute_specification_str}"'
        return q

    def load_query_str(self) -> str:
        q = f'LOAD_SUBTAR({self.plain_query_str()})'
        return q
