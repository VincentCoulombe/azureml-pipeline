# AzureML Wrapper

## C'est quoi
Une couche d'abstraction autour des classes du AzureML sdk pour Python. Permet de simplifier l'utilisation du dit sdk en prenant soin de faire la poutine habituelle à la place de l'utilisateur.

## Comment ça marche?
La suivante est un mini-tutoriel sur comment utiliser le Wrapper.

### Intéragir avec un Workspace
La classe WorkspaceWrapper permet à l'utilisateur de :

1. Créer/se connecter à un AzureML Workspace. Des deux manières suivantes :
  - `mon_workspace = WorkspaceWrapper(ws_name, resource_group, subscription_id)`
  - `config = {"ws_name":ws_name, "resource_group":resource_group, "subscription_id":subscription_id}`  
    `mon_workspace = WorkspaceWrapper.from_config(config)`
    
2. Accéder à toutes les méthodes et attributs de la classe [Workspace](https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.workspace.workspace?view=azure-ml-py) d'AzureML via:
  - `mon_workspace.ws` 
 
3. Enregistrer un [Environnement](https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.environment(class)?view=azure-ml-py) d'exécution custom dans le Workspace. Exemple :
  - `mon_workspace.register_env("test-env", ["pandas", "matplotlib"])`

4. Enregistrer un [Datastore](https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.datastore(class)?view=azure-ml-py) vers un [Azure Blob Container](https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction). Exemple : 
  - `mon_workspace.register_blob_datastore("test-datastore", "nom du container", "nom du blob", "clé d'accès")`

5. Enregistrer un csv dans le Workspace avec un Datastore. Exemple : 
  - `mon_workspace.register_csv("test-datastore", "test_dataset.csv")`

6. Enregistrer un [compute cluster](https://docs.microsoft.com/en-us/azure/machine-learning/how-to-create-attach-compute-cluster?tabs=python) dans le Workspace. Exemple :
  - `mon_workspace.register_compute("test-compute001")`  
  À noter qu'il est également possible de choisir la taille et le nombre de workers (min, max) du compute.
 
### Créer, Modifier, Lancer et Enregistrer des Pipelines et des Expériences 
Avec le Wrapper, il est très facile de lancer des expériences dans le cloud via des pipelines. Ces pipelines peuvent avoirs une ou plusieurs steps. Ces steps sont des scripts Python qui seront roulés dans le Cloud. Ces scripts devront commencés par la ligne de code suivante : `run = Run.get_context()` et finir par `run.complete()` pour pouvoir être track par le pipeline. Il est également possible de log dans [Run](https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.run(class)?view=azure-ml-py). De plus, chaque step va pouvoir avoir accès à des arguments. Les arguments possibles sont les suivants : 
  1. Une configuration, qui est un dictionnaire contenant les hyperparamètre de vos scripts (sera passé via step_config). Cette configuration sera accessible via l'argument `--config`
  2. Un ou des [Dataset(s)](https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.dataset.dataset?view=azure-ml-py) enregistrés dans le         Workspace du pipeline (sera passé via input_datasets)
  3. Des [outputfiledatasetconfig](https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.data.output_dataset_config.outputfiledatasetconfig?view=azure-ml-py) seront crées automatiquement pour communiquer de l'information entre vos steps. Chaque step recevra donc un `--input-foler` (sauf le premier step du pipeline) et un `--output-folder` (sauf le dernier step du pipeline).

Exemple 1) Script Python associé à un step du Pipeline. Ce step est un step intermédiaire du pipeline, il reçoit donc un input folder et devra retourner un output folder. Il reçoit aussi deux Datasets (test_data et new_data) en plus de la configuration.
```
run = Run.get_context()
parser = argparse.ArgumentParser()
parser.add_argument("--config", type=json.loads, dest="config") #Les hyperparamètres, est un dictionnaire!
parser.add_argument("--new-data", type=str, dest="new_data")
parser.add_argument("--test-data", type=str, dest="test_data")
parser.add_argument("--input-folder", type=str, dest="input_folder") #Folder contenant les données reçus de step précédent
parser.add_argument("--output-folder", type=str, dest="output_folder") #Folder dans lequel mettre les données à passer au prochain step
args = parser.parse_args()
config = args.config 
new_dataset = run.input_datasets[config.get("new_dataset_name")].to_pandas_dataframe() #Les datasets deviennent des DataFrames
test_dataset = run.input_datasets[config.get("test_dataset_name")].to_pandas_dataframe()

# TODO

run.complete()
```

Exemple 2)  Création d'un PipelineStep

Exemple 3) Création du même PipelineStep via .from_config

