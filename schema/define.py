from schema.tar import Tar, ImplicitTarDimensionSpecification, TarAttributeSpecification
from schema.schema import IntervalRange, IndexRange
from schema.dataset import FileDataset
from schema.subtar import OrderedSubTarDimensionSpecification, SubTarAttributeSpecification, SubTar


def implicit_tar_dimension(name: str, data_type, start, stop, step=1) -> ImplicitTarDimensionSpecification:
    """

    :param name:
    :param data_type:
    :param start:
    :param stop:
    :param step:
    :return:
    """

    return ImplicitTarDimensionSpecification(name, data_type, IntervalRange(start, stop, step))


def tar(name, dimensions, attributes) -> Tar:
    """

    :param name:
    :param dimensions:
    :param attributes:
    :return:
    """

    return Tar(name, dimensions, attributes)


def tar_attribute(name: str, data_type, length) -> TarAttributeSpecification:
    """

    :param name:
    :param data_type:
    :param length:
    :return:
    """

    return TarAttributeSpecification(name, data_type, length)


def ordered_subtar_dimension(dimension, start, stop, is_physical=False) -> OrderedSubTarDimensionSpecification:
    """

    :param dimension:
    :param start:
    :param stop:
    :param is_physical:
    :return:
    """

    return OrderedSubTarDimensionSpecification(dimension, IndexRange(start, stop, is_physical))


def subtar_attribute(attribute, dataset) -> SubTarAttributeSpecification:
    """

    :param attribute:
    :param dataset:
    :return:
    """
    return SubTarAttributeSpecification(attribute, dataset)


def subtar(tar, dimensions, attributes) -> SubTar:
    """

    :param tar:
    :param dimensions:
    :param attributes:
    :return:
    """
    return SubTar(tar, dimensions, attributes)


def file_dataset(name, path, data_type, is_in_savime_storage=False) -> FileDataset:
    return FileDataset(name=name, file_path=path, data_type=data_type, is_in_savime_storage=is_in_savime_storage)