from azureml.core import Run
import argparse
import json

class ScriptWrapper():
    def __init__(self) -> None:
        self.run = Run.get_context()
        self.parser = argparse.ArgumentParser
        
    @property
    def run(self):
        return self._run
    
    @run.setter
    def run(self, new_run:Run):
        if isinstance(new_run, Run):
            self._run = new_run
            
    def get_args(self):
        self.parser.add_argument("--config", type=json.loads, dest="config")
        self.parser.add_argument("--output-folder", type=str, dest="train_data_folder")
        args = self.parser.parse_args()
        new_dataset = self.run.input_datasets[args.config.get("new_dataset_name")].to_pandas_dataframe()
        test_dataset = self.run.input_datasets[args.config.get("test_dataset_name")].to_pandas_dataframe()
        return args.config, args.train_data_folder, [new_dataset, test_dataset]
        