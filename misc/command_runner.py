from typing import List, Tuple

from savime.client import Client
from schema.schema import CreatableSavimeElement, LoadableSavimeElement


class CommandRunner:

    def __init__(self, client: Client):
        self._client = client
        self.execute = client.execute

    def create(self, savime_element: CreatableSavimeElement):
        return self.execute(savime_element.create_query_str())

    def load(self, savime_element: LoadableSavimeElement):
        return self.execute(savime_element.load_query_str())

    def drop_tar(self, name):
        cmd = f'DROP_TAR("{name}");'
        return self.execute(cmd)

    def drop_dataset(self, name):
        cmd = f'DROP_DATASET("{name}");'
        return self.execute(cmd)

    @staticmethod
    def drop_type(self, name):
        cmd = f'DROP_TYPE("{name}");'
        return self.execute(cmd)

    def register_model(self, model_name: str, model_tar: str, input_attribute: str,
                       dim_specification: List[Tuple]):
        dim_specification_str = '|'.join(f'{dim_name}-{dim_size}' for dim_name, dim_size in dim_specification)
        cmd = f'REGISTER_MODEL({model_name}, {model_tar}, {input_attribute}, "{dim_specification_str}");'
        return self.execute(cmd)

    def predict(self, tar: str, model_name: str, input_attribute: str):
        cmd = f'PREDICT({tar}, {model_name}, {input_attribute});'
        return self.execute(cmd)