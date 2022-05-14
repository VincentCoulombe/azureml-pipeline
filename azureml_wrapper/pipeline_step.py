import json

import workspace_wrapper

class PipelineStep(workspace_wrapper.WorkspaceWrapper):
    MANDATORY_CONFIGS = ["ws_name", "resource_group", "subscription_id", "step_name", "script_name"]
    def __init__(self, ws_name:str, resource_group:str, subscription_id:str, step_name:str, script_name:str, step_config:dict=None, input_datasets:dict=None) -> None:
        """Créer une étape du Pipeline, les OutputFileDatasetConfigs sont créés automatiquement.

        Args:
            azureml_config (AzuremlWorkspace): Un instance d'AzuremlWorkspace
            name (str): Le nom de l'étape
            script_name (str): Le nom du script python associé à rouler
            config (dict): Les configurations à passer au script python. Ils seront passés via l'argument: --config

        """
        super().__init__(ws_name, resource_group, subscription_id)
        self.name = step_name
        if not script_name.endswith(".py"): script_name += ".py"
        self.script_name = script_name
        self.arguments = ["--config", json.dumps(step_config)] if isinstance(step_config, dict) else []
        if isinstance(input_datasets, dict):
            for input_arg_name, input_arg in input_datasets.items():
                data = self.ws.datasets.get(input_arg)
                if data is None: raise NameError(f"""Le Dataset {input_arg} n'est pas enregistré dans le workspace {self.ws.name} 
                                                    du resource groupe {self.ws.resource_group} de l'id {self.ws.subscription_id}.
                                                    Vous pouvez en enregistrer un via la méthode register_new_csv().""")
                self.arguments.extend([input_arg_name, data.as_named_input(input_arg)]) 
    
    @classmethod
    def from_config(cls, config:dict):
        if not isinstance(config, dict) : raise TypeError("config doit être un dict.")
        missing_keys = [key for key in cls.MANDATORY_CONFIGS if key not in config]
        if missing_keys:
            raise KeyError(f"Votre configuration doit contenir la (ou les) clée(s) suivante(s) : {missing_keys} pour être valide.")
        return cls(config.get("ws_name"), config.get("resource_group"), config.get("subscription_id"), config.get("step_name"),
                   config.get("script_name"), config.get("step_config"), config.get("input_datasets"))
