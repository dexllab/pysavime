from typing import List, Sequence, Tuple, Union

from schema.schema import CreatableSavimeElement, DroppableSavimeElement, LoadableSavimeElement
from schema.tar import Tar
from schema.dataset import Dataset


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
        t = [sequence[j] for j in range(i, i + n + 1)]
        yield tuple(t)


def _sequence_to_str(t):
    return ', '.join(ti for ti in t)


def create(savime_element: CreatableSavimeElement):
    return savime_element.create_query_str()


def load(savime_element: LoadableSavimeElement):
    return savime_element.load_query_str()


def drop(savime_element: DroppableSavimeElement):
    return savime_element.drop_query_str()


def register_model(model_name: str, model_tar: str, input_attribute: str,
                   dim_specification: List[Tuple]):
    dim_specification_str = '|'.join(f'{dim_name}-{dim_size}' for dim_name, dim_size in dim_specification)
    query = f'REGISTER_MODEL({model_name}, {model_tar}, {input_attribute}, "{dim_specification_str}");'
    return query


def predict(tar: str, model_name: str, input_attribute: str):
    query = f'PREDICT({tar}, {model_name}, {input_attribute});'
    return query


def select(tar: Union[Tar, str], *data_elements):
    tar_name = _get_tar_name(tar)
    data_elements_str = ', '.join([data_element for data_element in data_elements])
    query = f'SELECT({tar_name}, {data_elements_str})'
    return query


def where(tar: Union[Tar, str], logical_predicate):
    tar_name = _get_tar_name(tar)
    query = f'WHERE({tar_name}, {logical_predicate})'
    return query


def subset(tar, *dims):

    dims_str = ', '.join([_sequence_to_str(dim) for dim in _split_into_n(dims, 2)])
    tar_name = _get_tar_name(tar)
    query = f'SUBSET({tar_name}, {dims_str})'
    return query


def derive(tar, new_attribute_name, arithmetic_expression):
    tar_name = _get_tar_name(tar)
    query = f'DERIVE({tar_name}, {new_attribute_name}, {arithmetic_expression})'
    return query


def cross(left_tar, right_tar):
    left_tar_name = _get_tar_name(left_tar)
    right_tar_name = _get_tar_name(right_tar)
    query = f'CROSS({left_tar_name}, {right_tar_name})'
    return query


def dim_join(left_tar, right_tar, *tar_dims):

    left_tar_name = _get_tar_name(left_tar)
    right_tar_name = _get_tar_name(right_tar)

    dims_str = ', '.join([_sequence_to_str(dim_pair) for dim_pair in _split_into_n(tar_dims, 2)])
    query = f'DIMJOIN({left_tar_name}, {right_tar_name}, {dims_str})'
    return query


def aggregate(tar, *args):
    no_args = len(args)
    ix_dims = (no_args // 4) * 3

    tar_name = _get_tar_name(tar)
    arg_data_attr_str = ', '.join(_sequence_to_str(arg_triplet) for arg_triplet in _split_into_n(args[:ix_dims], 3))
    dim_attr_str = _sequence_to_str(args[ix_dims:])

    query = f'AGGREGATE({tar_name}, {arg_data_attr_str}, {dim_attr_str})'
    return query


def store(query, new_tar_name):
    query = f'STORE({query}, "{new_tar_name}")'
    return query

