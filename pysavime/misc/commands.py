from typing import Sequence, Tuple, Union

from pysavime.schema.schema import CreatableSavimeElement, DroppableSavimeElement, LoadableSavimeElement
from pysavime.schema.tar import TarDimensionSpecification, TarAttributeSpecification, Tar
from pysavime.schema.dataset import Dataset


def _get_name_of_named_element(element, element_class):
    if isinstance(element, element_class):
        return element.name
    if isinstance(element, str):
        return element


def _get_tar_name(tar):
    return _get_name_of_named_element(tar, Tar)


def _get_tar_dimension_name(dimension):
    return _get_name_of_named_element(dimension, TarDimensionSpecification)


def _get_tar_attribute_name(attribute):
    return _get_name_of_named_element(attribute, TarAttributeSpecification)


def _get_dataset_name(dataset):
    return _get_name_of_named_element(dataset, Dataset)


def _split_into_n(sequence: Sequence, n: int):
    assert len(sequence) % n == 0
    for i in range(0, len(sequence), n):
        t = [sequence[j] for j in range(i, i + n)]
        yield tuple(t)


def _sequence_to_str(t):
    return ', '.join(str(ti) for ti in t)


def create(savime_element: CreatableSavimeElement) -> str:
    """
    Get the create query string for a creatable elemetn.

    :param savime_element: A creatable savime element.
    :return: The create query for the savime_element.
    """
    return savime_element.create_query_str()


def load(savime_element: LoadableSavimeElement) -> str:
    """
    Get the load query string for a loadable element.

    :param savime_element: A loadable savime element.
    :return: The load query for the savime element.
    """
    return savime_element.load_query_str()


def drop(savime_element: DroppableSavimeElement) -> str:
    """
    Get the drop query string for a droppable element.

    :param savime_element: A droppable savime element.
    :return: The drop query for the savime element.
    """
    return savime_element.drop_query_str()


def register_model(model_identifier: str,
                   input_dim_specification: Sequence[Tuple[Union[TarDimensionSpecification, str], int]],
                   output_dim_specification:  Sequence[Tuple[Union[TarDimensionSpecification, str], int]],
                   attribute_specification: Sequence[Union[TarAttributeSpecification, str]]) -> str:
    """
    Get the register model query string for the informed model and its metadata.

    :param model_identifier: The model identifier registered in Savime.
    :param input_dim_specification: A sequence [(d1, s1), ..., (dn, sn)] of dimension-size pairs. Dimensions stand for
    the input tar dimension and size the number of elements in that dimension.
    :param output_dim_specification: A sequence [(d1, s1), ..., (dn, sn)] of dimension-size pairs. Dimensions stand for
    the tar output dimension and size the number of elements in that dimension.
    :param attribute_specification: A sequence of [(a1,...,an)] tar input attributes.
    :return: The register model query.
    """

    input_dim_spec_str = '|'.join(f'{_get_tar_dimension_name(dim)}-{dim_size}' for dim, dim_size in
                                  input_dim_specification)
    output_dim_spec_str = '|'.join(f'{_get_tar_dimension_name(dim)}-{dim_size}' for dim, dim_size in
                                   output_dim_specification)
    attribute_spec_str = ','.join(f'{_get_tar_attribute_name(attr)}' for attr in attribute_specification)

    query = f'REGISTER_MODEL({model_identifier}, "{input_dim_spec_str}", "{output_dim_spec_str}", "{attribute_spec_str}")'
    return query


def predict(tar: Union[Tar, str], model_identifier: str,):
    """
    Given a registered model, perform a prediction using as input the informed tar.

    :param tar: The input tar.
    :param model_identifier: The model identifier registered in Savime.
    :return: A predict query string.
    """
    tar_name = _get_tar_name(tar)

    query = f'PREDICT({tar_name}, {model_identifier})'
    return query


def select(tar: Union[Tar, str], *data_elements):
    """

    :param tar:
    :param data_elements:
    :return:
    """
    tar_name = _get_tar_name(tar)

    if len(data_elements) > 1:
        data_elements_str = ', '.join([data_element for data_element in data_elements])
        query = f'SELECT({tar_name}, {data_elements_str})'
    else:
        query = f'SELECT({tar_name})'
    return query


def where(tar: Union[Tar, str], logical_predicate):
    """

    :param tar:
    :param logical_predicate:
    :return:
    """
    tar_name = _get_tar_name(tar)
    query = f'WHERE({tar_name}, {logical_predicate})'
    return query


def subset(tar, *dims):
    """

    :param tar:
    :param dims:
    :return:
    """

    dims_str = ', '.join([_sequence_to_str(dim) for dim in _split_into_n(dims, 3)])
    tar_name = _get_tar_name(tar)
    query = f'SUBSET({tar_name}, {dims_str})'
    return query


def derive(tar, new_attribute_name, arithmetic_expression):
    """

    :param tar:
    :param new_attribute_name:
    :param arithmetic_expression:
    :return:
    """
    tar_name = _get_tar_name(tar)
    query = f'DERIVE({tar_name}, {new_attribute_name}, {arithmetic_expression})'
    return query


def cross(left_tar, right_tar):
    """

    :param left_tar:
    :param right_tar:
    :return:
    """
    left_tar_name = _get_tar_name(left_tar)
    right_tar_name = _get_tar_name(right_tar)
    query = f'CROSS({left_tar_name}, {right_tar_name})'
    return query


def dim_join(left_tar, right_tar, *tar_dims):
    """

    :param left_tar:
    :param right_tar:
    :param tar_dims:
    :return:
    """
    left_tar_name = _get_tar_name(left_tar)
    right_tar_name = _get_tar_name(right_tar)

    dims_str = ', '.join([_sequence_to_str(dim_pair) for dim_pair in _split_into_n(tar_dims, 2)])
    query = f'DIMJOIN({left_tar_name}, {right_tar_name}, {dims_str})'
    return query


def aggregate(tar, *args):
    """

    :param tar:
    :param args:
    :return:
    """

    tar_name = _get_tar_name(tar)

    no_args = len(args)
    if len(args) % 4 == 0:
        ix_dims = (no_args // 4) * 3
        dim_attr_str = _sequence_to_str(args[ix_dims:])
        arg_data_attr_str = ', '.join(_sequence_to_str(arg_triplet) for arg_triplet in _split_into_n(args[:ix_dims], 3))
        query = f'AGGREGATE({tar_name}, {arg_data_attr_str}, {dim_attr_str})'
    else:
        arg_data_attr_str = ', '.join(_sequence_to_str(arg_triplet) for arg_triplet in _split_into_n(args, 3))
        query = f'AGGREGATE({tar_name}, {arg_data_attr_str})'

    return query


def store(query, new_tar_name):
    """

    :param query:
    :param new_tar_name:
    :return:
    """
    query = f'STORE({query}, "{new_tar_name}")'
    return query
