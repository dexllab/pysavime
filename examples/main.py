from misc.command_runner import CommandRunner
from savime.client import Client
from savime.datatype import SavimeSupportedTypes
from schema.schema import IndexRange, IntervalRange, Range
from schema.dataset import RangeDataset
from schema.tar import ImplicitTarDimensionSpecification, TarAttributeSpecification, Tar
from schema.subtar import OrderedSubTarDimensionSpecification, SubTarAttributeSpecification, SubTar
from util.converter import DataVariableBlockConverter


def example():
    with Client(host='127.0.0.1', port=65000, raise_silent_error=True
                ) as client:
        command_runner = CommandRunner(client)

        attr_1_name, attr_2_name = 'attr_1', 'attr_2'
        dim_1_name, dim_2_name = 'dim_1', 'dim_2'
        dim_1_len, dim_2_len = 12500, 12500

        tar_name = 'tar'
        dataset_a_name = 'dataset_A'
        dataset_b_name = 'dataset_B'
        command_runner.execute('SELECT(joy);')
        command_runner.predict(tar='tar2', model_name='model', input_attribute='a')

        command_runner.drop_tar(tar_name)
        command_runner.drop_dataset(dataset_a_name)
        command_runner.drop_dataset(dataset_b_name)

        return

        dataset_a = RangeDataset(dataset_a_name, Range(1, dim_1_len * dim_2_len, 1), SavimeSupportedTypes.INT32)
        command_runner.create(dataset_a)
        dataset_b = RangeDataset(dataset_b_name, Range(1, dim_1_len * dim_2_len, 1), SavimeSupportedTypes.INT32)
        command_runner.create(dataset_b)

        dim_1_spec = ImplicitTarDimensionSpecification(dim_1_name, SavimeSupportedTypes.INT32,
                                                       IntervalRange(1, dim_1_len))
        dim_2_spec = ImplicitTarDimensionSpecification(dim_2_name, SavimeSupportedTypes.INT32,
                                                       IntervalRange(1, dim_2_len))
        attr_1_spec = TarAttributeSpecification(attr_1_name, dataset_a.data_type)
        attr_2_spec = TarAttributeSpecification(attr_2_name, dataset_b.data_type)

        tar = Tar(tar_name, [dim_1_spec, dim_2_spec], [attr_1_spec, attr_2_spec])
        command_runner.create(tar)

        st_dim_1_spec = OrderedSubTarDimensionSpecification(dim_1_spec, IndexRange(1, dim_1_len, False))
        st_dim_2_spec = OrderedSubTarDimensionSpecification(dim_2_spec, IndexRange(1, dim_2_len, False))
        st_attr_1_spec = SubTarAttributeSpecification(attr_1_spec, dataset_a)
        st_attr_2_spec = SubTarAttributeSpecification(attr_2_spec, dataset_b)

        sub_tar = SubTar(tar, [st_dim_1_spec, st_dim_2_spec], [st_attr_1_spec, st_attr_2_spec])
        command_runner.load(sub_tar)

        to_pandas_data_frame = DataVariableBlockConverter('pandas')
        to_xarray = DataVariableBlockConverter('xarray')
        for response in client.execute(f'select({tar.name})'):
            #print(response)
            print(to_pandas_data_frame(response).info())
            print(to_xarray(response))


def main():
    example()


if __name__ == '__main__':
    main()
