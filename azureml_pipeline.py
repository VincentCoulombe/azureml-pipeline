from azureml.core import Workspace, Datastore, Environment, Dataset, Experiment
from azureml.data import OutputFileDatasetConfig
from azureml.pipeline.steps import PythonScriptStep
from azureml.pipeline.core import Pipeline
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.core.compute_target import ComputeTargetException
from azureml.core.runconfig import RunConfiguration
import json
import yaml

class AzuremlWorkspace():
    def __init__(self, workspace:Workspace, environment:Environment, compute_name:str, compute_size:str, compute_min_nodes:int=0, compute_max_nodes:int=1) -> None:
        if not isinstance(workspace, Workspace): raise TypeError("workspace doit être un Workspace")
        if not isinstance(environment, Environment): raise TypeError("environment doit être un Environment")
        self.ws = workspace
        self.env = environment
        self.ds = None
        
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
    
    def register_new_csv(self, datastore:Datastore, dataset_name:str) -> None:
        if not isinstance(datastore, Datastore): raise TypeError("datastore doit être un Datastore")
        if dataset_name.endswith(".csv"): dataset_name = dataset_name[:-4]
        path = [(datastore,f"{dataset_name}.csv")]
        data = Dataset.Tabular.from_delimited_files(path=path)
        data.register(workspace=self.ws, name=dataset_name, create_new_version=True)
        

class PipelineStep():
    def __init__(self, azureml_config:AzuremlWorkspace, step_name:str, script_name:str, step_config:dict=None, input_datasets:dict=None) -> None:
        """Créer une étape du Pipeline, les OutputFileDatasetConfigs sont créés automatiquement.

        Args:
            name (str): Le nom de l'étape
            script_name (str): Le nom du script python associé à rouler
            config (dict): Les configurations à passer au script python. Ils seront passés via l'argument: --config
            input_datasets (dict): Les datasets à passer au script python. Les items seront passés dans des arguments ayant comme nom les clées. 
                                    Ces datasets doivent êtres dans le workspace du AzuremlWorkspace passé en argument avec ce StepConfig.
        """
        self.azureml_config = azureml_config
        self.name = step_name
        if not script_name.endswith(".py"): script_name += ".py"
        self.script_name = script_name
        self.arguments = ["--config", json.dumps(step_config)] if isinstance(step_config, dict) else []
        if isinstance(input_datasets, dict):
            for input_arg_name, input_arg in input_datasets.items():
                data = self.azureml_config.ws.datasets.get(input_arg) #Gère cette erreur (data not registered)
                self.arguments.extend([input_arg_name, data.as_named_input(input_arg)]) 

class AzuremlPipeline():
    def __init__(self, azureml_ws:AzuremlWorkspace, *steps) -> None:
        if not isinstance(azureml_ws, AzuremlWorkspace): raise TypeError("azureml_config doit être un AzuremlWorkspace")
        self.azureml_ws = azureml_ws
        
        self.run_config = RunConfiguration() 
        self.run_config.environment = self.azureml_ws.env
        if not self.azureml_ws.compute: raise TypeError("""Vous devez avoir un compute de provisionner dans azureml_ws pour pouvoir lancer un AzuremlPipeline.
                                                        Vous pouvez provisionner un compute via la méthode provision_compute d'AzuremlWorkspace.""")
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
        
if __name__ == '__main__':
    ws = Workspace("7240386a-1717-43ab-9acf-f5fccc41ddbb",
                   "analyse-sentiment-kara",
                   "as-kara")
    env = Environment.get(ws,"deep-learning")
    
    ws = AzuremlWorkspace(ws, env, "gpu-compute001", "Standard_NC6")
    
    prep_step = PipelineStep(ws, "data prep", "data_prep.py", step_config={"storage_acc_name": "analysesentiment",
                                                                    "storage_acc_key": "fi7WJU/dsYPstX//gkSBihTmUGRKzNb37uDOrOuZHnpRHm37spFPzhYxI0DkhEs+/E9MDsDbdoPyQzif4Z2XAA==",
                                                                    "container_name": "sondage",
                                                                    "new_dataset_name": "testing_sdk",
                                                                    "train_dataset_name": "testing_sdk_train",
                                                                    "test_dataset_name": "testing_sdk_test",
                                                                    "labels_col_name": "CategoriePhrase",
                                                                    "texts_col_name": "PhrasesEN",
                                                                    "test_size": 0.1,
                                                                    "shuffle": True,
                                                                    "max_text_lenght": 512,
                                                                    "min_text_lenght": 5}, 
                                                                    input_datasets={"--new-data": "testing_sdk",
                                                                                    "--test-data": "testing_sdk_test"})
    
    train_step = PipelineStep(ws, "train test save", "classifier_finetuner.py", step_config={"lr": 0.00002,
                                                                                            "epochs": 1,
                                                                                            "text_lenght": 512,
                                                                                            "batch_size": 16,
                                                                                            "nb_labels": 4,
                                                                                            "sup_dataset_name": "testing_sdk_train", 
                                                                                            "test_dataset_name": "testing_sdk_test",
                                                                                            "labels_col_name": "CategoriePhrase",
                                                                                            "texts_col_name": "PhrasesEN",
                                                                                            "name": "test_model",
                                                                                            "HuggingFace_name": "distilbert-base-uncased"})
    
    deploy_step = PipelineStep(ws, "deploy", "deployer_model.py", step_config={"env_name": "deep-learning",
                                                                                "model_names": ["test_model"],
                                                                                "web_service_name": "test",
                                                                                "inference_script_name": "test_inference_script.py",
                                                                                "cpu_cores": 1,
                                                                                "memory": 3})
    pipeline = AzuremlPipeline(ws, prep_step, train_step, deploy_step)
    pipeline.run("test")


    