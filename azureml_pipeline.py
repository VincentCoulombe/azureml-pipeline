from azureml.core import Workspace, Datastore, Environment, Dataset, Experiment
from azureml.data import OutputFileDatasetConfig
from azureml.pipeline.steps import PythonScriptStep
from azureml.pipeline.core import Pipeline
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException
from azureml.core.runconfig import RunConfiguration
import json

class AzuremlWorkspace():
    def __init__(self,ws_name:str, resource_group:str, subscription_id:str, env_name:str,
                 compute_name:str, compute_size:str="Standard_NC6", compute_min_nodes:int=0, compute_max_nodes:int=1) -> None:
        self.ws = Workspace.get(name=ws_name,
                            subscription_id=subscription_id,
                            resource_group=resource_group)
        
        self.env = Environment.get(self.ws,env_name)        
        self._init_compute(compute_name, compute_size, compute_min_nodes, compute_max_nodes)
    
    @classmethod
    def from_dict(cls, **config):
        return cls(config.get("ws_name"), config.get("resource_group"), config.get("subscription_id"), 
                                config.get("env_name"), config.get("compute_name"), config.get("compute_size"), 
                                config.get("compute_min_nodes"), config.get("compute_max_nodes"))
  
    @property
    def ws(self):
        return self._ws
            
    @ws.setter
    def ws(self, new_ws:Workspace):
        self._ws = new_ws
    
    @property
    def env(self):
        return self._env 
    
    @env.setter
    def env(self, new_env:Environment):
        self._env = new_env      
    
    @property
    def compute(self):
        return self._compute
    
    @compute.setter
    def compute(self, new_compute:ComputeTarget):
        self._compute = new_compute
    
    def register_new_csv(self, datastore_name:Datastore, dataset_name:str) -> None:
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
    
    def provision_compute(self, compute_name:str, compute_size:str="Standard_NC6", compute_min_nodes:int=0, compute_max_nodes:int=1):
        self._init_compute(compute_name, compute_size, compute_min_nodes, compute_max_nodes)
        
    def _init_compute(self, compute_name:str, compute_size:str, compute_min_nodes:int=0, compute_max_nodes:int=1):
        try:
            compute = ComputeTarget(workspace=self.ws, name=compute_name)
        except ComputeTargetException:
            compute_config = AmlCompute.provisioning_configuration(vm_size=compute_size,
                                                                    min_nodes=compute_min_nodes,
                                                                    max_nodes=compute_max_nodes)
            compute = ComputeTarget.create(self.ws, compute_name, compute_config)
        except UnboundLocalError as e:
            print(f"Erreur lors du provisionnement du compute {compute_name}: {e}")
            quit()        
        finally:    
            compute.wait_for_completion(show_output=True)
        self.compute = compute
        

class PipelineStep():
    def __init__(self, azureml_config:AzuremlWorkspace, step_name:str, script_name:str, step_config:dict=None, input_datasets:dict=None) -> None:
        """Créer une étape du Pipeline, les OutputFileDatasetConfigs sont créés automatiquement.

        Args:
            name (str): Le nom de l'étape
            script_name (str): Le nom du script python associé à rouler
            config (dict): Les configurations à passer au script python. Ils seront passés via l'argument: --config
            input_datasets (dict): Les datasets à passer au script python. Les items seront passés dans des arguments ayant comme nom les clées.
                                    Par exemple, input_datasets[--new-data] =  new_dataset.
                                    Ces datasets doivent êtres dans le workspace du AzuremlWorkspace passé en argument avec ce StepConfig.
        """
        self.azureml_config = azureml_config
        self.name = step_name
        if not script_name.endswith(".py"): script_name += ".py"
        self.script_name = script_name
        self.arguments = ["--config", json.dumps(step_config)] if isinstance(step_config, dict) else []
        if isinstance(input_datasets, dict):
            for input_arg_name, input_arg in input_datasets.items():
                data = self.azureml_config.ws.datasets.get(input_arg)
                if data is None: raise NameError(f"""Le Dataset {input_arg} n'est pas enregistré dans le workspace {self.azureml_config.ws.name} 
                                                    du resource groupe {self.azureml_config.ws.resource_group} de l'id {self.azureml_config.ws.subscription_id}.
                                                    Vous pouvez en enregistrer un via la méthode register_new_csv() d'AzuremlWorkspace.""")
                self.arguments.extend([input_arg_name, data.as_named_input(input_arg)]) 

class AzuremlPipeline():
    def __init__(self, azureml_ws:AzuremlWorkspace, *steps) -> None:
        if not isinstance(azureml_ws, AzuremlWorkspace): raise TypeError("azureml_config doit être un AzuremlWorkspace")
        self.azureml_ws = azureml_ws
        
        self.run_config = RunConfiguration() 
        self.run_config.environment = self.azureml_ws.env
        if not self.azureml_ws.compute: raise TypeError("""Vous devez avoir un compute de provisionner dans votre AzuremlWorkspace pour pouvoir lancer un AzuremlPipeline.
                                                        Vous pouvez provisionner un compute via la méthode provision_compute() d'AzuremlWorkspace.""")
        self.run_config.target = self.azureml_ws.compute 
                
        self.pipeline_steps = []
        output_folder = OutputFileDatasetConfig("folder0")
        input_folder = None
        for i, step in enumerate(steps):
            if isinstance(step, PipelineStep):
                if step == steps[-1]: output_folder = None
                self._add_step(step, input_folder=input_folder, output_folder=output_folder)
                input_folder = output_folder
                output_folder = OutputFileDatasetConfig(f"folder{i+1}")    
   
    def _add_step(self, step:PipelineStep, input_folder:OutputFileDatasetConfig=None,
                  output_folder:OutputFileDatasetConfig=None) -> None:
        if input_folder: step.arguments.extend(["--input-folder", input_folder.as_input()])
        if output_folder: step.arguments.extend(["--output-folder", output_folder])

        self.pipeline_steps.append(PythonScriptStep(name = step.name,
                                    source_directory = ".",
                                    script_name = step.script_name,
                                    arguments = step.arguments,
                                    compute_target = self.azureml_ws.compute,
                                    runconfig = self.run_config,
                                    allow_reuse = True))
        
    def run(self, experiment_name:str)->None:
        pipeline = Pipeline(workspace=self.azureml_ws.ws, steps=self.pipeline_steps)
        experiment = Experiment(workspace=self.azureml_ws.ws, name=experiment_name)
        run = experiment.submit(pipeline)
        run.wait_for_completion()
        




    