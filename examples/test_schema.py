import os
from schema.dataset import *
from schema.subtar import *
from schema.tar import *

from savime.client import Client
from misc.command_runner import CommandRunner
from util.converter import DataVariableBlockConverter
from util.data_variable import DataVariableBlock

from typing import Iterable


data_dir = '/media/daniel/Data/git/savime/examples'

# Paths
base_f_path = os.path.join(data_dir, 'base')

# Ranges
r1 = Range(2, 6, 2, 1)

# Literals
l1 = Literal(values=[1.5, 3.2, 4.7, 7.9, 13.1], data_type=SavimeSupportedTypes.FLOAT)
l2 = Literal(values=[2, 8], data_type=SavimeSupportedTypes.INT32)
l3 = Literal(values=[3, 1, 4], data_type=SavimeSupportedTypes.INT64)
l4 = Literal(values=[1, 2], data_type=SavimeSupportedTypes.INT64)
l5 = Literal(values=[2, 10, 4, 2, 6, 8], data_type=SavimeSupportedTypes.INT32)
l6 = Literal(values=[2, 2, 4, 8, 8, 10], data_type=SavimeSupportedTypes.INT32)
l7 = Literal(values=[4, 2, 3, 1, 0], data_type=SavimeSupportedTypes.INT64)
l8 = Literal(values=[0, 2, 3, 2, 4], data_type=SavimeSupportedTypes.INT64)
l9 = Literal(values=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10], data_type=SavimeSupportedTypes.INT32)
l10 = Literal(values=["apples", "oranges", "bananas", "melons", "pears", "peaches"],
              data_type=SavimeSupportedTypes.CHAR)

literals = [l1, l2, l3, l4, l5, l6, l7, l8, l9, l10]

# Datasets
base = FileDataset(name='base', data_type=SavimeSupportedTypes.DOUBLE, file_path=base_f_path,
                   is_in_savime_storage=False)
dsexplict = LiteralDataset(name='dsexplict', literal=l1)
dspart1 = RangeDataset(name='dspart1', data_type=SavimeSupportedTypes.INT32, range=r1)
dspart2 = LiteralDataset(name='dspart2', literal=l2)
dsepart1 = LiteralDataset(name='dsepart1', literal=l3)
dsepart2 = LiteralDataset(name='dsepart2', literal=l4)
dstotalimplicitx = LiteralDataset(name='dstotalimplicitx', literal=l5)
dstotalimplicity = LiteralDataset(name='dstotalimplicity', literal=l6)
dstotalexplicitx = LiteralDataset(name='dstotalexplicitx', literal=l7)
dstotalexplicity = LiteralDataset(name='dstotalexplicity', literal=l8)
vec = LiteralDataset(name='vec', literal=l9, num_columns=2)
vecstr = LiteralDataset(name='vecstr', literal=l10, num_columns=10)

datasets = [base, dsexplict, dspart1, dspart2, dsepart1, dsepart2, dstotalimplicitx, dstotalimplicity,
            dstotalexplicitx, dstotalexplicity, vec, vecstr]

# Types
t1 = TarMetaType(name='footype', dimension_names=['dim1', 'dim2'], attribute_names=['val'])
types = [t1]

# Tars
ta0_dx = ImplicitTarDimensionSpecification(name='x', data_type=SavimeSupportedTypes.INT32,
                                           interval=IntervalRange(0, 10, 1))
ta0_dy = ImplicitTarDimensionSpecification(name='y', data_type=SavimeSupportedTypes.INT32,
                                           interval=IntervalRange(0, 10, 1))
ta1_dx = ImplicitTarDimensionSpecification(name='x', data_type=SavimeSupportedTypes.INT32,
                                           interval=IntervalRange(0, 10, 2))
ta1_dy = ImplicitTarDimensionSpecification(name='y', data_type=SavimeSupportedTypes.INT32,
                                           interval=IntervalRange(0, 10, 2))

ta2_dx = ImplicitTarDimensionSpecification(name='x', data_type=SavimeSupportedTypes.FLOAT,
                                           interval=IntervalRange(0.0, 10.0, 0.5))
ta2_dy = ImplicitTarDimensionSpecification(name='y', data_type=SavimeSupportedTypes.FLOAT,
                                           interval=IntervalRange(0.0, 10.0, 0.5))

ta3_dx = ExplicitTarDimensionSpecification(name='x', dataset=dsexplict.name)
ta3_dy = ExplicitTarDimensionSpecification(name='y', dataset=dsexplict.name)

ta4_di = ImplicitTarDimensionSpecification(name='i', interval=IntervalRange(1, 5, 1),
                                           data_type=SavimeSupportedTypes.INT32)

ta5_di = ImplicitTarDimensionSpecification(name='i', interval=IntervalRange(0, 5, 1),
                                           data_type=SavimeSupportedTypes.INT32)

ta0_mapping = {
    ta0_dx.name: 'dim1',
    ta0_dy.name: 'dim2',
    'a': 'val'
}

ta1_mapping = {
    ta1_dx.name: 'dim1',
    ta1_dy.name: 'dim2',
    'a': 'val'
}

ta2_mapping = {
    ta2_dx.name: 'dim1',
    ta2_dy.name: 'dim2',
    'a': 'val'
}

a1 = TarAttributeSpecification(name='a', data_type=SavimeSupportedTypes.DOUBLE)
a2 = TarAttributeSpecification(name='a', data_type=SavimeSupportedTypes.INT32, num_columns=2)
a3 = TarAttributeSpecification(name='s', data_type=SavimeSupportedTypes.CHAR, num_columns=10)

io = Tar(name='io', dimension_specification=[ta0_dx, ta0_dy], attribute_specification=[a1], meta_type=t1,
         meta_type_mapping=ta0_mapping)

io2 = Tar(name='io2', dimension_specification=[ta1_dx, ta1_dy], attribute_specification=[a1],
          meta_type=t1, meta_type_mapping=ta1_mapping)

iohalf = Tar(name='iohalf', dimension_specification=[ta2_dx, ta2_dy], attribute_specification=[a1],
             meta_type=t1, meta_type_mapping=ta2_mapping)

ip = Tar(name='ip', dimension_specification=[ta1_dx, ta1_dy], attribute_specification=[a1])

it = Tar(name='it', dimension_specification=[ta1_dx, ta1_dy], attribute_specification=[a1])

eo = Tar(name='eo', dimension_specification=[ta3_dx, ta3_dy], attribute_specification=[a1])

ep = Tar(name='ep', dimension_specification=[ta3_dx, ta3_dy], attribute_specification=[a1])

et = Tar(name='et', dimension_specification=[ta3_dx, ta3_dy], attribute_specification=[a1])

vi = Tar(name='vi', dimension_specification=[ta4_di], attribute_specification=[a2])

vs = Tar(name='vs', dimension_specification=[ta5_di], attribute_specification=[a3])

tars = [io, io2, iohalf, ip, it, eo, ep, et, vi, vs]

# SubTARS
stt1 = OrderedSubTarDimensionSpecification(dimension='x', index_range=IndexRange(0, 2, True))
stt2 = OrderedSubTarDimensionSpecification(dimension='y', index_range=IndexRange(0, 2, True))
stt3 = OrderedSubTarDimensionSpecification(dimension='y', index_range=IndexRange(3, 5, True))
stt4 = OrderedSubTarDimensionSpecification(dimension='x', index_range=IndexRange(3, 5, True))

stt5 = OrderedSubTarDimensionSpecification(dimension='x', index_range=IndexRange(0, 4, False))
stt6 = OrderedSubTarDimensionSpecification(dimension='x', index_range=IndexRange(6, 10, False))
stt7 = OrderedSubTarDimensionSpecification(dimension='y', index_range=IndexRange(0, 4, False))
stt8 = OrderedSubTarDimensionSpecification(dimension='y', index_range=IndexRange(6, 10, False))

stt9 = OrderedSubTarDimensionSpecification(dimension='x', index_range=IndexRange(1.5, 4.7, False))
stt10 = OrderedSubTarDimensionSpecification(dimension='y', index_range=IndexRange(1.5, 4.7, False))
stt11 = OrderedSubTarDimensionSpecification(dimension='x', index_range=IndexRange(7.9, 13.1, False))
stt12 = OrderedSubTarDimensionSpecification(dimension='y', index_range=IndexRange(7.9, 13.1, False))

stt13 = OrderedSubTarDimensionSpecification(dimension='x', index_range=IndexRange(0, 6, False))
stt14 = OrderedSubTarDimensionSpecification(dimension='x', index_range=IndexRange(8, 10, False))

stt15 = PartialSubTarDimensionSpecification(dimension='y', index_range=IndexRange(0, 10, False),
                                            dataset=dspart1)
stt16 = PartialSubTarDimensionSpecification(dimension='y', index_range=IndexRange(0, 10, False),
                                            dataset=dspart2)

stt17 = PartialSubTarDimensionSpecification(dimension='y', index_range=IndexRange(1.5, 13.1, False),
                                            dataset=dsepart1)

stt18 = TotalSubTarDimensionSpecification(dimension='x', index_range=IndexRange(0, 10, False),
                                          dataset=dstotalimplicitx)

stt19 = TotalSubTarDimensionSpecification(dimension='y', index_range=IndexRange(0, 10, False),
                                          dataset=dstotalimplicity)

stt20 = TotalSubTarDimensionSpecification(dimension='x', index_range=IndexRange(1.5, 13.1, False),
                                          dataset=dstotalexplicitx)

stt21 = TotalSubTarDimensionSpecification(dimension='y', index_range=IndexRange(1.5, 13.1, False),
                                          dataset=dstotalexplicity)

stt22 = OrderedSubTarDimensionSpecification(dimension='i', index_range=IndexRange(0, 4, True))
stt23 = OrderedSubTarDimensionSpecification(dimension='i', index_range=IndexRange(0, 5, True))

a = SubTarAttributeSpecification(attribute='a', dataset=base.name)
a2 = SubTarAttributeSpecification(attribute='a', dataset=vec.name)
a3 = SubTarAttributeSpecification(attribute='s', dataset=vecstr.name)

st1 = SubTar(io, dimension_specification=[stt1, stt2], attribute_specification=[a])
st2 = SubTar(io, dimension_specification=[stt1, stt3], attribute_specification=[a])
st3 = SubTar(io, dimension_specification=[stt4, stt2], attribute_specification=[a])
st4 = SubTar(io, dimension_specification=[stt4, stt3], attribute_specification=[a])

st5 = SubTar(io2, dimension_specification=[stt5, stt7], attribute_specification=[a])
st6 = SubTar(io2, dimension_specification=[stt5, stt8], attribute_specification=[a])
st7 = SubTar(io2, dimension_specification=[stt6, stt7], attribute_specification=[a])
st8 = SubTar(io2, dimension_specification=[stt6, stt8], attribute_specification=[a])

st9 = SubTar(iohalf, dimension_specification=[stt1, stt2], attribute_specification=[a])
st10 = SubTar(iohalf, dimension_specification=[stt1, stt3], attribute_specification=[a])
st11 = SubTar(iohalf, dimension_specification=[stt4, stt2], attribute_specification=[a])
st12 = SubTar(iohalf, dimension_specification=[stt4, stt3], attribute_specification=[a])

st13 = SubTar(eo, dimension_specification=[stt9, stt10], attribute_specification=[a])
st14 = SubTar(eo, dimension_specification=[stt9, stt12], attribute_specification=[a])
st15 = SubTar(eo, dimension_specification=[stt11, stt10], attribute_specification=[a])
st16 = SubTar(eo, dimension_specification=[stt11, stt12], attribute_specification=[a])

st17 = SubTar(ip, dimension_specification=[stt13, stt15], attribute_specification=[a])
st18 = SubTar(ip, dimension_specification=[stt14, stt16], attribute_specification=[a])

st19 = SubTar(ep, dimension_specification=[stt9, stt17], attribute_specification=[a])
st20 = SubTar(ep, dimension_specification=[stt11, stt17], attribute_specification=[a])

st21 = SubTar(it, dimension_specification=[stt18, stt19], attribute_specification=[a])
st22 = SubTar(et, dimension_specification=[stt20, stt21], attribute_specification=[a])

st23 = SubTar(vi, dimension_specification=[stt22], attribute_specification=[a2])
st24 = SubTar(vs, dimension_specification=[stt23], attribute_specification=[a3])

subtars = [st1, st2, st3, st4, st5, st6, st7, st8, st9, st10, st11, st12, st13, st14, st15, st16, st17, st18, st19,
           st20, st21, st22, st23, st24]

savime_port, savime_host = 65000, '127.0.0.1'


def print_response_as_xarray(responses: Iterable[DataVariableBlock]):
    converter = DataVariableBlockConverter('xarray')
    for response in responses:
        print(converter(response))


with Client(port=savime_port, host=savime_host) as client:
    command_runner = CommandRunner(client)

    for literal in literals:
        print(literal.plain_query_str())

    for dataset in datasets:
        print_response_as_xarray(command_runner.create(dataset))

    for type_ in types:
        print_response_as_xarray(command_runner.create(type_))

    for tar in tars:
        print_response_as_xarray(command_runner.create(tar))

    for subtar_ in subtars:
        print_response_as_xarray(command_runner.load(subtar_))

    from examples.queries import queries

    for query in queries:
        print_response_as_xarray(command_runner.execute(query))