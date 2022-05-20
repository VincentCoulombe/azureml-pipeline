from azureml.core import Run
import argparse
import json
import pandas as pd
import os
import sys


class ScriptWrapper():
    def __init__(self) -> None:
        self.run = Run.get_context()
        self.parser = argparse.ArgumentParser()
        self.args_list = []
        for arg in sys.argv[1:]:
            if arg.startswith("--"):
                if str(arg) == "--config":
                    self.parser.add_argument("--config", type=json.loads, dest="config")
                    self.args_list.append("config")
                else:
                    arg_name = str(arg).replace("--", "").replace("-", "_")
                    self.parser.add_argument(str(arg), type=str, dest=arg_name)
                    self.args_list.append(arg_name)
        self.args = self.parser.parse_args()

    @property
    def run(self):
        return self._run

    @run.setter
    def run(self, new_run: Run):
        if isinstance(new_run, Run):
            self._run = new_run

    def get_config(self):
        if "config" not in self.args_list:
            raise ValueError(f"config n'est pas dans la liste d'arguments reçus. Soit : {self.args_list}.")
        return self.args.config

    def get_csv_from_input_folder(self, csv_name: str) -> pd.DataFrame:
        if "input_folder" not in self.args_list:
            raise ValueError("""Aucun input folder de reçu en argument.
                             Ce script est probablement la première étape du pipeline.
                             Utilisez get_csv_from_config() si vous souhaitez charger un Dataset
                             qui ne provient pas d'une étape précédente du pipeline.""")
        if not csv_name.endswith(".csv"):
            csv_name += ".csv"
        return pd.read_csv(os.path.join(self.args.input_folder, csv_name))

    def save_csv_in_output_folder(self, dataframe: pd.DataFrame, saving_name: str, index: bool = False, header: bool = True):
        if "output_folder" not in self.args_list:
            raise ValueError("""Aucun output folder de reçu en argument. Ce script est probablement la dernière étape du pipeline.""")
        if not saving_name.endswith(".csv"):
            saving_name += ".csv"
        folder = self.args.output_folder
        os.makedirs(folder, exist_ok=True)
        save_path = os.path.join(folder, saving_name)
        dataframe.to_csv(save_path, index=index, header=header)

    def get_csv_from_config(self, csv_name: str) -> pd.DataFrame:
        if csv_name.endswith(".csv"):
            csv_name.replace(".csv", "")
        if csv_name not in self.get_config().values():
            raise NameError(f"{csv_name} n'est pas dans config. Voici le contenu de config : {self.get_config()}.")
        return self.run.input_datasets[csv_name].to_pandas_dataframe()
