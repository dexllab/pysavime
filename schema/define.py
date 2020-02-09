from schema.dataset import *
from schema.subtar import *


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

    :param name: The implicit tar dimension name.
    :param data_type: The data type associated to this dimension.
    :param start: The first element in this dimension.
    :param stop: The last element in this dimension.
    :param step: The distance between two consecutive elements in this dimension.
    :return: The schema for a implicit tar dimension.
    """

    checked_data_type = _to_savime_supported_type(data_type)
    return ImplicitTarDimensionSpecification(name, checked_data_type, IntervalRange(start, stop, step))


def tar(name: str, dimensions: Sequence[TarDimensionSpecification],
        attributes: Sequence[TarAttributeSpecification]) -> Tar:

    """

    :param name:
    :param dimensions:
    :param attributes:
    :return:
    """

    return Tar(name, dimensions, attributes)


def tar_attribute(name: str, data_type: Union[str, SavimeSupportedTypes], length=1) -> TarAttributeSpecification:
    """

    :param name:
    :param data_type:
    :param length:
    :return:
    """

    checked_data_type = _to_savime_supported_type(data_type)
    return TarAttributeSpecification(name, checked_data_type, length)


def ordered_subtar_dimension(dimension: TarDimensionSpecification, start, stop, is_physical=False) \
        -> OrderedSubTarDimensionSpecification:
    """

    :param dimension:
    :param start:
    :param stop:
    :param is_physical:
    :return:
    """

    return OrderedSubTarDimensionSpecification(dimension, IndexRange(start, stop, is_physical))


def subtar_attribute(attribute: TarAttributeSpecification, dataset: Dataset) -> SubTarAttributeSpecification:
    """

    :param attribute:
    :param dataset:
    :return:
    """
    return SubTarAttributeSpecification(attribute, dataset)


def subtar(tar: Tar, dimensions: Sequence[SubTarDimensionSpecification],
           attributes: Sequence[SubTarAttributeSpecification]) -> SubTar:
    """

    :param tar:
    :param dimensions:
    :param attributes:
    :return:
    """
    return SubTar(tar, dimensions, attributes)


def file_dataset(name: str, path: str, data_type, is_in_savime_storage=False, length: int = 1) -> FileDataset:
    checked_data_type = _to_savime_supported_type(data_type)
    return FileDataset(name=name, file_path=path, data_type=checked_data_type,
                       is_in_savime_storage=is_in_savime_storage, num_columns=length)