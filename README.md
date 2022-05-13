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

4. Enregistrer un [Datastore](https://docs.microsoft.com/en-us/python/api/azureml-core/azureml.core.datastore(class)?view=azure-ml-py) vers un [Azure Blob Container] (https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction). Exemple : 
  - `mon_workspace.register_blob_datastore("test-datastore", "nom du container", "nom du blob", "clé d'accès")`

5. Enregistrer un csv dans le Workspace avec un Datastore. Exemple : 
  - `mon_workspace.register_csv("test-datastore", "test_dataset.csv")`

6. Enregistrer un [compute cluster](https://docs.microsoft.com/en-us/azure/machine-learning/how-to-create-attach-compute-cluster?tabs=python) dans le Workspace. Exemple :
  - `mon_workspace.register_compute("test-compute001")`
  À noter qu'il est également possible de choisir la taille et le nombre de workers (min, max) du compute.
 
