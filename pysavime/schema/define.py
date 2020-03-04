from pysavime.schema.dataset import *
from pysavime.schema.subtar import *
from pysavime.schema.tar import *


def _to_savime_supported_type(input_data_type):
    if not isinstance(input_data_type, SavimeSupportedTypes):

        try:
            input_data_type = input_data_type.upper()
            output_data_type = SavimeSupportedTypes.__members__[input_data_type]
        except KeyError:
            print(f'SAVIME does not support the "{input_data_type}" data type. Please choose one among '
                  f'{list(SavimeSupportedTypes.__members__.keys())}')
            raise
    else:
        output_data_type = input_data_type

    return output_data_type


def implicit_tar_dimension(name: str, data_type: Union[str, SavimeSupportedTypes],
                           start, stop, step=1) -> ImplicitTarDimensionSpecification:
    """
    Create an implicit tar dimension specification.

    :param name: The implicit tar dimension name.
    :param data_type: The data type associated to this dimension.
    :param start: The first element in this dimension.
    :param stop: The last element in this dimension.
    :param step: The distance between two consecutive elements in this dimension.
    :return: The schema for an implicit tar dimension.
    """

    checked_data_type = _to_savime_supported_type(data_type)
    return ImplicitTarDimensionSpecification(name, checked_data_type, IntervalRange(start, stop, step))


def explicit_tar_dimension(name: str, dataset: [str, Dataset]) -> ExplicitTarDimensionSpecification:
    """
    Create an explicit tar dimension specified by the dataset.

    :param name: The explicit tar dimension specification.
    :param dataset: The dataset which specifies the dimension.
    :return: The schema for an explicit tar dimension.
    """
    return ExplicitTarDimensionSpecification(name, dataset)


def tar_metatype(name: str, dimension_names: Sequence[str], attribute_names: Sequence[str]) -> TarMetaType:
    """
    Create a meta type for dimension specifications.

    :param name: The meta type name.
    :param dimension_names: The meta type dimension names.
    :param attribute_names: The meta type attribute names.
    :return: The meta type.
    """

    return TarMetaType(name, dimension_names, attribute_names)


def tar(name: str, dimensions: Sequence[TarDimensionSpecification], attributes: Sequence[TarAttributeSpecification])\
        -> Tar:
    """
    Create a tar schema specification.

    :param name: The tar name.
    :param dimensions: The dimension specifications.
    :param attributes: The attribute specifications.
    :return: The tar schema specification.
    """

    return Tar(name, dimensions, attributes)


def tar_attribute(name: str, data_type: Union[str, SavimeSupportedTypes], length=1) -> TarAttributeSpecification:
    """
    Create a tar attribute schema specification.

    :param name: The attribute name.
    :param data_type: The attribute data type.
    :param length: The number of vector dimensions the attribute has.
    :return: The tar attribute schema specification.
    """

    checked_data_type = _to_savime_supported_type(data_type)
    return TarAttributeSpecification(name, checked_data_type, length)


def ordered_subtar_dimension(dimension: TarDimensionSpecification, start, stop, is_physical: bool=False) \
        -> OrderedSubTarDimensionSpecification:
    """
    Create an ordered subtar dimension specification.

    :param dimension: The subtar dimension.
    :param start: The subtar start.
    :param stop: The subtar stop.
    :param is_physical: If set to True, start and stop refer to physical indices, which are memory positions in the
    underlying dimension. If set to False, start and stop refer to logical indices, which are defined by the dimension.
    :return: A specification for an ordered subtar dimension.
    """

    return OrderedSubTarDimensionSpecification(dimension=dimension, index_range=IndexRange(start, stop, is_physical))


def partial_subtar_dimension(dimension: TarDimensionSpecification, dataset: Union[Dataset, str],
                             start, stop, is_physical: bool = False) -> PartialSubTarDimensionSpecification:
    """
    Create a partial subtar dimension specification.

    :param dimension: The subtar dimension.
    :param dataset: The dataset containing the values for the dimension specification.
    :param start: The subtar stop
    :param stop: The subtar stop.
    :param is_physical: If set to True, start and stop refer to physical indices, which are memory positions in the
    underlying dimension. If set to False, start and stop refer to logical indices, which are defined by the dimension.
    :return: A specification for a partial subtar dimension.
    """
    return PartialSubTarDimensionSpecification(dimension=dimension, dataset=dataset,
                                               index_range=IndexRange(start, stop, is_physical))


def total_subtar_dimension(dimension: TarDimensionSpecification, dataset: Union[Dataset, str],
                           start, stop, is_physical: bool = False) -> TotalSubTarDimensionSpecification:
    """
    Create a total subtar dimension specification.

    :param dimension: The subtar dimension.
    :param dataset: The dataset containing the values for the dimension specification.
    :param start: The subtar stop
    :param stop: The subtar stop.
    :param is_physical: If set to True, start and stop refer to physical indices, which are memory positions in the
    underlying dimension. If set to False, start and stop refer to logical indices, which are defined by the dimension.
    :return: A specification for a total subtar dimension.
    """
    return TotalSubTarDimensionSpecification(dimension=dimension, dataset=dataset,
                                             index_range=IndexRange(start, stop, is_physical))


def subtar_attribute(attribute: Union[TarAttributeSpecification, str], dataset: Union[Dataset, str]) \
        -> SubTarAttributeSpecification:
    """
    Create a subtar attribute specification.

    :param attribute: The subtar attribute.
    :param dataset: The dataset containing the attribute values.
    :return: A specification for a subtar attribute specification.
    """
    return SubTarAttributeSpecification(attribute, dataset)


def subtar(tar_: Tar, dimensions: Sequence[SubTarDimensionSpecification],
           attributes: Sequence[SubTarAttributeSpecification]) -> SubTar:
    """
    Create a subtar specification.

    :param tar_: The target tar.
    :param dimensions: The subtar dimension specification.
    :param attributes: The subtar attribute specification.
    :return: A specification for subtar.
    """
    return SubTar(tar_, dimensions, attributes)


def file_dataset(name: str, path: str, data_type: Union[str, SavimeSupportedTypes], is_in_savime_storage=False,
                 length: int = 1) -> FileDataset:
    """
    Create the specification for a file dataset.

    :param name: The dataset name.
    :param path: The dataset path.
    :param data_type: The dataset data type.
    :param is_in_savime_storage: Indicates whether or not the underlying dataset is in savime storage.
    :param length: The number of vector dimensions the dataset has.
    :return: A specification for a file dataset.
    """
    checked_data_type = _to_savime_supported_type(data_type)
    return FileDataset(name=name, file_path=path, data_type=checked_data_type,
                       is_in_savime_storage=is_in_savime_storage, num_columns=length)


def literal_dataset(name: str, values: Sequence, data_type: Union[str, SavimeSupportedTypes],
                    length: int = 1) -> LiteralDataset:
    """
    Create the specification for a literal dataset.

    :param name: The dataset name.
    :param values: The dataset values.
    :param data_type: The dataset data type.
    :param length: The number of vector dimensions the dataset has.
    :return: A specification for a literal dataset.
    """
    checked_data_type = _to_savime_supported_type(data_type)
    return LiteralDataset(name=name, literal=Literal(values=values, data_type=checked_data_type), num_columns=length)


def range_dataset(name: str, start, stop, step, num_repetitions: int, data_type: Union[str, SavimeSupportedTypes])\
        -> RangeDataset:
    """
    Create the specification for a range dataset.

    :param name: The dataset name.
    :param start: The range start.
    :param stop: The range stop.
    :param step: The range step.
    :param num_repetitions: The number of repetitions for the range.
    :param data_type: The range data type.
    :return: A specification for a range dataset.
    """
    checked_data_type = _to_savime_supported_type(data_type)
    return RangeDataset(name=name, range=Range(start=start, stop=stop, step=step, num_repetitions=num_repetitions),
                        data_type=checked_data_type)
