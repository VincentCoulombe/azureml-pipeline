from azureml.core import Workspace, Datastore, Environment, Dataset, Experiment
from azureml.data import OutputFileDatasetConfig
from azureml.pipeline.steps import PythonScriptStep
from azureml.pipeline.core import Pipeline, Schedule, ScheduleRecurrence
from azureml.core.conda_dependencies import CondaDependencies
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException
from azureml.core.runconfig import RunConfiguration
from azureml.exceptions import ProjectSystemException
import json

class AzuremlWorkspace():
    MANDATORY_CONFIGS = ["ws_name", "resource_group", "subscription_id"]
    def __init__(self,ws_name:str, resource_group:str, subscription_id:str) -> None:
        """Instancie un AzuremlWorkspace qui permet d'accéder à un Workspace, un Environment et un ComputeTarget.

        Args:
            ws_name (str): Le nom du Workspace
            resource_group (str): Le nom du Resource Group
            subscription_id (str): L'id de l'utilisateur
            env_name (str): Le nom de l'Environment (doit être enregistré dans le Workspace ws_name)
            compute_name (str): Le nom du ComputeTarget.
            compute_size (str, optional): La taille du ComputeTarget. Defaults to "Standard_NC6".
            compute_min_nodes (int, optional): Le nombre de workers minimum du ComputeTarget. Defaults to 0.
            compute_max_nodes (int, optional): Le nombre de workers maximum du ComputeTarget. Defaults to 1.
        """
        try:
            self.ws = Workspace.get(subscription_id=subscription_id,
                                    resource_group=resource_group,
                                    name=ws_name) 
        except ProjectSystemException as e:
            try:
                self.ws = Workspace.create(name=ws_name,
                        subscription_id=subscription_id,
                        resource_group=resource_group,
                        create_resource_group=False)
            except ProjectSystemException:
                    self.ws = Workspace.create(name=ws_name,
                        subscription_id=subscription_id,
                        resource_group=resource_group,
                        create_resource_group=True)
        
    @classmethod
    def from_config(cls, config:dict):
        if not isinstance(config, dict) : raise TypeError("config doit être un dict.")
        missing_keys = [key for key in cls.MANDATORY_CONFIGS if key not in config]
        if missing_keys:
            raise KeyError(f"Votre configuration doit contenir la (ou les) clée(s) suivante(s) : {missing_keys} pour être valide.")
        return cls(config.get("ws_name"), config.get("resource_group"), config.get("subscription_id"))
  
    @property
    def ws(self):
        return self._ws
            
    @ws.setter
    def ws(self, new_ws:Workspace):
        if isinstance(new_ws, Workspace):
            self._ws = new_ws
        
    def register_env(self, name:str, dependencies:list):
        if not isinstance(dependencies, list) : raise TypeError("Le paramètre dependencies doit être une liste.")
        if name in self.ws.environments: print(f"L'Environnement {name} est déjà enregistré dans le Workspace {self.ws.name}.")        
        else: 
            environment = Environment(name)
            environment.python.conda_dependencies = CondaDependencies.create(pip_packages=[])
            environment.register(self.ws)
        
    def register_blob_datastore(self, name:str, container_name:str, storage_name:str, storage_key:str):
        if name in self.ws.datastores: print(f"Le Datastore {name} est déjà enregistré dans le Workspace {self.ws.name}.")        
        else: Datastore.register_azure_blob_container(workspace=self.ws,
                                                    datastore_name=name,
                                                    account_name=storage_name,
                                                    container_name=container_name,
                                                    account_key=storage_key)
    
    def register_csv(self, datastore_name:str, dataset_name:str) -> None:  
        if dataset_name in self.ws.compute_targets: print(f"Le Dataset {dataset_name} est déjà enregistré dans le Workspace {self.ws.name}.") 
        else:     
            if datastore_name in self.ws.datastores:
                datastore = Datastore.get(self.ws, datastore_name)
            else:
                raise NameError(f"""Le Datastore {datastore_name} n'est pas enregistré dans le workspace {self.ws.name} 
                                    du resource groupe {self.ws.resource_group} de l'id {self.ws.subscription_id}.
                                    Vous pouvez en enregistrer un via le browser web d'AzureML.""")
            if dataset_name.endswith(".csv"): dataset_name = dataset_name[:-4]
            path = [(datastore,f"{dataset_name}.csv")]
            data = Dataset.Tabular.from_delimited_files(path=path)
            data.register(workspace=self.ws, name=dataset_name, create_new_version=True)
    
    def register_compute(self, name:str, compute_size:str="Standard_NC6", compute_min_nodes:int=0, compute_max_nodes:int=1):
        if name in self.ws.compute_targets: print(f"Le Compute {name} est déjà enregistré dans le Workspace {self.ws.name}.") 
        else:
            compute_config = AmlCompute.provisioning_configuration(vm_size=compute_size,
                                                                    min_nodes=compute_min_nodes,
                                                                    max_nodes=compute_max_nodes)
            compute = ComputeTarget.create(self.ws, name, compute_config)
            compute.wait_for_completion(show_output=True)
        

        

class PipelineStep(AzuremlWorkspace):
    MANDATORY_CONFIGS = ["ws_name", "resource_group", "subscription_id", "step_name", "script_name"]
    def __init__(self, ws_name:str, resource_group:str, subscription_id:str, step_name:str, script_name:str, step_config:dict=None, input_datasets:dict=None) -> None:
        """Créer une étape du Pipeline, les OutputFileDatasetConfigs sont créés automatiquement.

        Args:
            azureml_config (AzuremlWorkspace): Un instance d'AzuremlWorkspace
            name (str): Le nom de l'étape
            script_name (str): Le nom du script python associé à rouler
            config (dict): Les configurations à passer au script python. Ils seront passés via l'argument: --config
            input_datasets (dict): Les datasets à passer au script python. Les items seront passés dans des arguments ayant comme nom les clées.
                                    Par exemple, input_datasets[--new-data] =  new_dataset.
                                    Ces datasets doivent êtres dans le workspace du AzuremlWorkspace passé en argument avec ce StepConfig.
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

class AzuremlPipeline(AzuremlWorkspace):
    MANDATORY_CONFIGS = ["ws_name", "resource_group", "subscription_id", "env_name", "compute_name", "steps"]
    def __init__(self, ws_name:str, resource_group:str, subscription_id:str, env_name:str, compute_name:str, steps:list) -> None:
        if not isinstance(steps, list) : raise TypeError("steps doit être une liste, même si elle ne contient qu'un step.")
        super().__init__(ws_name, resource_group, subscription_id)
        self.run_config = RunConfiguration()
        self.run_config.environment = Environment.get(self.ws,env_name)
        try:
            self.compute = ComputeTarget(workspace=self.ws, name=compute_name)
        except ComputeTargetException as e:
            raise TypeError(f"Le compute {compute_name} n'est pas enregistré dans le Workspace {self.ws.name}. Vous pouvez le faire via la méthode register_compute().") from e

        self.run_config.target = self.compute
        self.steps = steps
        self.pipeline_steps = []
        self.output_folder = OutputFileDatasetConfig()
        self.input_folder = None
        for step in self.steps:
            self.add_step(step)

    @property
    def pipeline(self):
        return self._pipeline
            
    @pipeline.setter
    def pipeline(self, new_pipeline:Pipeline):
        if isinstance(new_pipeline, Pipeline):
            self._pipeline = new_pipeline
            
    @classmethod
    def from_config(cls, config:dict):
        if not isinstance(config, dict) : raise TypeError("config doit être un dict.")
        missing_keys = [key for key in cls.MANDATORY_CONFIGS if key not in config]
        if missing_keys:
            raise KeyError(f"Votre configuration doit contenir la (ou les) clée(s) suivante(s) : {missing_keys} pour être valide.")
        base_config = {"ws_name":config.get("ws_name"), "resource_group":config.get("resource_group"), "subscription_id":config.get("subscription_id")}
        steps_config = config.get("steps")
        steps= []
        for _, step_config in steps_config.items():
            step_config = {**base_config, **step_config}
            steps.append(PipelineStep.from_config(step_config))
           
        return cls(config.get("ws_name"), config.get("resource_group"), config.get("subscription_id"), config.get("env_name"), config.get("compute_name"), steps)
   
    def add_step(self, step:PipelineStep):  
        if not isinstance(step, PipelineStep): raise TypeError("Le paramètre step doit être un instance de la classe PipelineStep.")
        if self.ws.name != step.ws.name: raise ValueError(f"Le Workspace du paramètre step ({step.ws.name}) doit être le même que celui du pipeline ({self.ws.name})")
        
        
        if step != self.steps[-1]: step.arguments.extend(["--output-folder", self.output_folder])
        if self.input_folder: step.arguments.extend(["--input-folder", self.input_folder.as_input()])
        self.pipeline_steps.append(PythonScriptStep(name = step.name,
                            source_directory = ".",
                            script_name = step.script_name,
                            arguments = step.arguments,
                            compute_target = self.compute,
                            runconfig = self.run_config,
                            allow_reuse = True))
        self.input_folder = self.output_folder
        self.output_folder = OutputFileDatasetConfig()
    
        
    def run(self, experiment_name:str)->None:
        if len(self.pipeline_steps)>0:
            Pipeline(workspace=self.ws, steps=self.pipeline_steps)
            self.experiment = Experiment(workspace=self.ws, name=experiment_name)
            self.run = self.experiment.submit(self._pipeline)
            self.run.wait_for_completion()
        
    def register(self, name:str, description:str, schedule:str=None, interval:int=None, datastore_name:str=None):
        self.pipeline = self.run.publish_pipeline(name=name, description=description, version="0")
        if schedule:
            if schedule not in self.POSSIBLE_SCHEDULES: raise ValueError(f"Les schédules possibles sont : {self.POSSIBLE_SCHEDULES}.")
            if schedule == "On_blob_change":
                if not datastore_name: raise ValueError(f"Avec la schédule {schedule}, vous devez préciser un datastore_name vers le blob en question.")
                datastore = Datastore.get(workspace=self.ws, name=datastore_name)
                schedule = Schedule.create(self.ws, name=f"{name}Schedule", pipeline_id=self.pipeline.id,
                                            experiment_name=self.experiment.name, datastore=datastore)
            else:
                if not interval: raise ValueError(f"Avec la schédule {schedule}, un interval de {schedule}.")
                recurrence = ScheduleRecurrence(frequency=schedule, interval=interval)
                schedule = Schedule.create(self.ws, name=f"{name}Schedule", 
                                                description=description,
                                                pipeline_id=self.pipeline.id, 
                                                experiment_name=self.experiment.name, 
                                                recurrence=recurrence)
    