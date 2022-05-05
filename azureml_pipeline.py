from azureml.core import Workspace, Datastore, Environment, Dataset, Experiment
from azureml.data import OutputFileDatasetConfig
from azureml.pipeline.steps import PythonScriptStep
from azureml.pipeline.core import Pipeline
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException
from azureml.core.runconfig import RunConfiguration
import json
import yaml

class AzuremlPipeline():
    def __init__(self, filepath:str) -> None:
        with open(filepath, "r") as file_descriptor:
            data = yaml.load(file_descriptor) 
            
        self.data_config = data.get("data_config")
        self.steps_config = data.get("steps_config")  
        self.azure_config = data.get("azure_config")
        self.compute_config = data.get("compute_config")
          
        self.ws, self.env, self.run_name = self._azureml_setup()
        self.compute_name, self.compute = self._compute_setup()
        
        
        
        self.run_config = RunConfiguration()
        if self.compute_name: self.run_config.target = self.compute
        if self.env: self.run_config.environment = self.env
        
        self.pipeline_steps = []
        self._add_steps()
        
    
    def _azureml_setup(self)->tuple:
        ws = Workspace(self.azure_config.get("subscription_id"),
                    self.azure_config.get("resource_group"),
                    self.azure_config.get("workspace_name"))
        env = Environment.get(ws,self.azure_config.get("environment_name")) #Gère cette erreur (pas tjrs besoin d'un env)
        ds = Datastore(ws, self.data_config.get("datastore_name")) #Gère cette erreur (pas tjrs besoin d'un ds)
        datasets_name = self.data_config.get("datasets")
        storage_dataset_name = datasets_name.get("to_import")
        path = [(ds,f"{storage_dataset_name}.csv")]
        data = Dataset.Tabular.from_delimited_files(path=path)
        data.register(workspace=ws, name=storage_dataset_name, create_new_version=True)
        return ws, env, self.azure_config.get("experiment_name")
    
    def _compute_setup(self)->tuple:
        if isinstance(self.compute_config, dict):
            compute_name = self.compute_config.get("name")
            try:
                compute = ComputeTarget(workspace=self.ws, name=compute_name)
                print("Va utiliser le compute gpu déjà provisionné")
            except ComputeTargetException:
                self.compute_config = AmlCompute.provisioning_configuration(vm_size=self.compute_config.get("type"),
                                                                        min_nodes=0,
                                                                        max_nodes=1)
                compute = ComputeTarget.create(self.ws, compute_name, self.compute_config)
                print("Va provisionner un nouveau compute gpu")
            except UnboundLocalError as e:
                print(f"Veuillez relancer le script, erreur critique: {e}")
                quit()        
            finally:    
                compute.wait_for_completion(show_output=True)
        else: compute_name = ""
        return compute_name, compute
    
    def _add_steps(self)->None:
        for step_config in self.steps_config:
            self.add_step(step_config)

    def _add_step(self, step_config:dict)->None:
        data_folder = OutputFileDatasetConfig("data_folder")
        arguments = ["--data-folder", data_folder]
        
        inputs = step_config.get("inputs")
        if isinstance(inputs, dict):
            for input_arg_name, input_arg in inputs.items():
                data = self.ws.datasets.get(input_arg) #Gère cette erreur (data not registered)
                arguments.extend(input_arg_name, data.as_named_input(input_arg))
        script_config = step_config.get("script_config")
        
        arguments.extend([script_config.get("arg_name"), json.dumps(script_config)])
                
        self.pipeline_steps.append(PythonScriptStep(name = step_config.get("name"),
                                    source_directory = ".",
                                    script_name = step_config.get("script"),
                                    arguments = arguments,
                                    compute_target = self.compute,
                                    runconfig = self.run_config,
                                    allow_reuse = True))
        
    def run(self)->None:
        pipeline = Pipeline(workspace=self.ws, steps=self.pipeline_steps)
        experiment = Experiment(workspace=self.ws, name=self.run_name)
        run = experiment.submit(pipeline)
        run.wait_for_completion()


    