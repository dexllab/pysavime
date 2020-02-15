from typing import List, Sequence, Tuple, Union

from pysavime.schema.schema import CreatableSavimeElement, DroppableSavimeElement, LoadableSavimeElement
from pysavime.schema.tar import Tar
from pysavime.schema.dataset import Dataset


def _get_name_of_named_element(element, element_class):
    if isinstance(element, element_class):
        return element.name
    if isinstance(element, str):
        return element


def _get_tar_name(tar):
    return _get_name_of_named_element(tar, Tar)


def _get_dataset_name(dataset):
    return _get_name_of_named_element(dataset, Dataset)


def _split_into_n(sequence: Sequence, n: int):
    assert len(sequence) % n == 0
    for i in range(0, len(sequence), n):
        t = [sequence[j] for j in range(i, i + n)]
        yield tuple(t)


def _sequence_to_str(t):
    return ', '.join(str(ti) for ti in t)


def create(savime_element: CreatableSavimeElement):
    """

    :param savime_element:
    :return:
    """
    return savime_element.create_query_str()


def load(savime_element: LoadableSavimeElement):
    """

    :param savime_element:
    :return:
    """
    return savime_element.load_query_str()


def drop(savime_element: DroppableSavimeElement):
    """

    :param savime_element:
    :return:
    """
    return savime_element.drop_query_str()


def register_model(model_name: str, model_tar: str, input_attribute: str,
                   dim_specification: List[Tuple]):
    """

    :param model_name:
    :param model_tar:
    :param input_attribute:
    :param dim_specification:
    :return:
    """
    dim_specification_str = '|'.join(f'{dim_name}-{dim_size}' for dim_name, dim_size in dim_specification)
    query = f'REGISTER_MODEL({model_name}, {model_tar}, {input_attribute}, "{dim_specification_str}");'
    return query


def predict(tar: str, model_name: str, input_attribute: str):
    """

    :param tar:
    :param model_name:
    :param input_attribute:
    :return:
    """
    query = f'PREDICT({tar}, {model_name}, {input_attribute});'
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
    no_args = len(args)
    ix_dims = (no_args // 4) * 3

    tar_name = _get_tar_name(tar)
    arg_data_attr_str = ', '.join(_sequence_to_str(arg_triplet) for arg_triplet in _split_into_n(args[:ix_dims], 3))
    dim_attr_str = _sequence_to_str(args[ix_dims:])

    query = f'AGGREGATE({tar_name}, {arg_data_attr_str}, {dim_attr_str})'
    return query


def store(query, new_tar_name):
    """

    :param query:
    :param new_tar_name:
    :return:
    """
    query = f'STORE({query}, "{new_tar_name}")'
    return query
