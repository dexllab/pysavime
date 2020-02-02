from collections import namedtuple, OrderedDict
import numpy as np
import xarray as xr
from typing import List, Sequence, Tuple, Union
import os

from savime.datatype import SavimeDataTypeUtility
import schema.schema
from util.queue import Queue
from util.data_variable import DataVariable, DataVariableBlock


def custom_cum_sum(lengths):
    repeats = np.ones(len(lengths), dtype=np.int64)
    tiles = np.ones(len(lengths), dtype=np.int64)

    for j in range(len(lengths)-2, -1, -1):
        repeats[j] = lengths[j+1] * repeats[j+1]

    for i in range(1, len(lengths)):
        tiles[i] = lengths[i-1] * tiles[i-1]

    return repeats, tiles


def comb_cross_product(arrays):

    lengths = [len(array) for array in arrays]
    repeats, tiles = custom_cum_sum(np.array(lengths, dtype=np.int64))
    ixs = [np.tile(np.repeat(array, repeats[i]), tiles[i]) for i, array in enumerate(arrays)]

    return ixs


class RandomDatasetSplitter:

    def __init__(self, dataset: xr.Dataset, max_inner_cuts: [int, Sequence]):
        self._dataset = dataset
        self._max_inner_cuts = max_inner_cuts

    def process_dataset(self):
        data_variable_blocks = []
        for data_array_name, data_array in self._dataset.items():
            data_variable_blocks.extend(self.process_data_array(data_array))

        return data_variable_blocks

    def process_data_array(self, data_array: xr.DataArray):

        names, shape = zip(*[(coord_name, len(coord_array)) for coord_name, coord_array in data_array.coords.items()])
        NT = namedtuple('random_slice', names)
        random_slices = tuple([NT(*li) for li in self.get_random_slices(shape, self._max_inner_cuts)])

        data_variable_blocks = []

        for random_slice in random_slices:
            ixs = {}
            for key, value in zip(names, random_slice):
                ix = slice(value[0], value[1] + 1, 1)
                ixs[key] = ix

            random_slice_data_array = xr.DataArray = data_array.isel(**ixs)
            flat_dim_arrays = comb_cross_product(random_slice_data_array.coords.values())
            dims = OrderedDict({dim_name: dim_f_array.reshape(-1, 1) for dim_name, dim_f_array in
                                zip(random_slice_data_array.coords.keys(), flat_dim_arrays)})
            attrs = OrderedDict({data_array.name: random_slice_data_array.values.reshape(-1, 1)})

            random_slice_data_variable = DataVariableBlock(dims=dims, attrs=attrs)
            data_variable_blocks.append(random_slice_data_variable)

        return data_variable_blocks

    @classmethod
    def get_random_slices(cls, shape: Tuple, max_inner_cuts: Union[int, Sequence]) -> List[List[Tuple]]:
        """
        Given the shape of a multi dimensional array and the maximum number of cuts to apply on each dimension, this
        method yields a random list of slices l, where len(l)[0] = len(shape) and 0 < len(l) <= prod(max_inner_cuts).

        Examples:
            >>> np.random.seed(32)
            >>> shape = (10, 5, 7)
            >>> max_cuts = 2
            >>> RandomDatasetSplitter.get_random_slices(shape, 2)
            ... [[(0, 4), (0, 1), (0, 6)], [(0, 4), (2, 4), (0, 6)], [(5, 9), (0, 4), (0, 1)], [(5, 9), (0, 4), (2, 6)]]

        Args:
            shape: The shape of a multi dimensional array.
            max_inner_cuts: The maximum number of divisions each dimension can be divided into. It can be an integer
            (every dimension has the same number of maximum divisions, or a sequence of length equal to the shape's
            length.

        Returns:
            The computed random slices.

        """

        if not isinstance(max_inner_cuts, list):
            max_inner_cuts = [max_inner_cuts] * len(shape)

        if isinstance(max_inner_cuts, list):
            assert len(max_inner_cuts) == len(shape), 'The length of max_inner_cuts must be equal to the length of' \
                                                      'shape.'

        # The queue stores the slices. Each slice is progressively built, that is to say, the first dimension is 'cut',
        # then the second, and so on.

        q = Queue()
        for random_slice in cls.get_dim_random_slices(shape[0], max_inner_cuts[0]):
            q.push([random_slice])

        while True:
            current_slice = q.front()
            current_dimension = len(current_slice)

            if current_dimension < len(shape):
                q.pop()
                for random_slice in cls.get_dim_random_slices(shape[current_dimension], max_inner_cuts[current_dimension]):
                    q.push(current_slice + [random_slice])
            else:
                break

        return list(q)

    @classmethod
    def get_dim_random_slices(cls, dim_length: int, max_inner_cuts: int) -> List[Tuple]:
        """
        Get random slices for a particular dimension.

        Examples:
            >>> np.random.seed(32)
            >>> RandomDatasetSplitter.get_dim_random_slices(10, 3)
            ... [(0, 2), (3, 6), (7, 9)]

        Args:
            dim_length: The dimension length.
            max_inner_cuts: The maximum number of inner cuts.

        Returns:

        """

        assert max_inner_cuts >= 0, 'The maximum number of cuts must greater or equal to 0.'
        assert isinstance(max_inner_cuts, int), 'The maximum number of cuts must be an integer.'

        points = set()
        # The actual number of inner cuts has to be less than them dimension size - 1, so that every cut gets at least one
        # element
        num_inner_cuts = np.random.randint(0, min(dim_length - 2, max_inner_cuts))
        num_cuts = 0

        while num_cuts < num_inner_cuts:
            point = np.random.randint(1, dim_length - 2)
            # if point - 1 not in points and point + 1 not in points:
            len_bf = len(points)
            points.add(point)
            if len_bf < len(points):
                num_cuts += 1

        points.add(-1)
        points.add(dim_length - 1)

        sorted_points = sorted(points)
        slices = [(sorted_points[i] + 1, sorted_points[i+1]) for i in range(len(sorted_points)-1)]

        return slices


def data_variable_block_to_sub_tars(data_variable_block: DataVariableBlock, sub_tars_dir_path: str):
    for key, value in data_variable_block.dims.items():
        path = os.path.join(sub_tars_dir_path, key)
        value.tofile(path)
        savime_dtype = SavimeDataTypeUtility.numpy_to_savime_primitive(value.dtype)
        savime_dataset = schema.FileDataset(key, savime_dtype, path, False, value.shape[1])

    for key, value in data_variable_block.attrs.items():
        path = os.path.join(sub_tars_dir_path, key)
        value.tofile(path)
        savime_dtype = SavimeDataTypeUtility.numpy_to_savime_primitive(value.dtype)
        savime_dataset = schema.FileDataset(key, savime_dtype, path, False, value.shape[1])


def data_variable_to_dataset(data_variable: DataVariable, dir_path: str):
    name = data_variable.name
    dtype = SavimeDataTypeUtility.numpy_to_savime_primitive(data_variable.array.dtype)
    length = data_variable.array.shape[1]
    path = os.path.join(dir_path, name)

    dataset = schema.FileDataset(name, dtype, path, False, length)
    print(dataset.create_query_str())


class DatasetToTar:

    def __init__(self, dataset: xr.Dataset, random_split: bool = True):
        self._dataset = dataset
        self.process()

    def process(self):
        splitter = RandomDatasetSplitter(self._dataset, 10)
        data_variable_blocks = splitter.process_dataset()
        print(data_variable_blocks[0])
        # schema.FileDataset()
        # schema.ExplicitTarDimensionSpecification()
        # schema.Tar()


def create_example_dataset(longitude_steps=722, latitude_steps=362, time_steps=10):

    longitude_range = (-180, 180, longitude_steps)
    latitude_range = (-90, 90, latitude_steps)

    longitude = np.linspace(*longitude_range, dtype=np.float32)
    latitude = np.linspace(*latitude_range, dtype=np.float32)
    time = np.arange(time_steps)

    temperature = 15 + 10 * np.random.rand(longitude_steps, latitude_steps, time_steps)
    precipitation = 10 * np.random.rand(longitude_steps, latitude_steps, time_steps)

    temperature_xr = xr.DataArray(temperature, coords=(('lon', longitude), ('lat', latitude), ('time', time)),
                     name='temperature', attrs={'scale': 'celsius'})
    precipitation_xr = xr.DataArray(precipitation, coords=(('lon', longitude), ('lat', latitude), ('time', time)),
                     name='precipitation', attrs={'scale': 'mm'})

    dataset = xr.Dataset({temperature_xr.name: temperature_xr, precipitation_xr.name: precipitation_xr})
    dataset.attrs['name'] = 'temperature_and_precipitation'

    return dataset


def main():

    np.random.seed(32)
    dataset = create_example_dataset()
    splitter = RandomDatasetSplitter(dataset, 3)
    dataset_to_tar = DatasetToTar(dataset)


if __name__ == '__main__':
    main()
