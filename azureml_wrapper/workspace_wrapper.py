from __future__ import annotations
from typing import Any, Dict, List

from azureml.core import Workspace, Datastore, Environment, Dataset
from azureml.core.conda_dependencies import CondaDependencies
from azureml.core.compute import ComputeTarget, AmlCompute
from azureml.exceptions import ProjectSystemException, WorkspaceException


class WorkspaceWrapper():
    MANDATORY_CONFIGS = ["ws_name", "resource_group", "subscription_id"]

    def __init__(self, ws_name: str, resource_group: str, subscription_id: str) -> None:
        """Instancie un WorkspaceWrapper qui permet d'accéder à un Workspace, un Environment et un ComputeTarget.

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
        except ProjectSystemException:
            try:
                self.ws = Workspace.create(name=ws_name,
                                           subscription_id=subscription_id,
                                           resource_group=resource_group,
                                           create_resource_group=False)
            except WorkspaceException:
                self.ws = Workspace.create(name=ws_name,
                                           subscription_id=subscription_id,
                                           resource_group=resource_group,
                                           create_resource_group=True,
                                           location="eastus")

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> WorkspaceWrapper:
        if not isinstance(config, Dict):
            raise TypeError("config doit être un dictionnaire.")
        missing_keys = [key for key in cls.MANDATORY_CONFIGS if key not in config]
        if missing_keys:
            raise KeyError(f"Votre configuration doit contenir la (ou les) clée(s) suivante(s) : {missing_keys} pour être valide.")
        return cls(str(config.get("ws_name")), str(config.get("resource_group")), str(config.get("subscription_id")))

    @property
    def ws(self) -> Workspace:
        return self._ws

    @ws.setter
    def ws(self, new_ws: Workspace) -> None:
        if isinstance(new_ws, Workspace):
            self._ws = new_ws

    def register_env(self, name: str, dependencies: List[str]) -> None:
        if not isinstance(dependencies, List):
            raise TypeError("Le paramètre dependencies doit être une liste.")
        if name in self.ws.environments:
            print(f"L'Environnement {name} est déjà enregistré dans le Workspace {self.ws.name}.")
        else:
            environment = Environment(name)
            environment.python.conda_dependencies = CondaDependencies.create(pip_packages=dependencies)
            environment.register(self.ws)

    def register_blob_datastore(self, name: str, container_name: str, storage_name: str, storage_key: str) -> None:
        if name in self.ws.datastores:
            print(f"Le Datastore {name} EST enregistré dans le Workspace {self.ws.name}.")
        else:
            Datastore.register_azure_blob_container(workspace=self.ws,
                                                    datastore_name=name,
                                                    account_name=storage_name,
                                                    container_name=container_name,
                                                    account_key=storage_key)

    def unregister_blob_datastore(self, name: str) -> None:
        if name not in self.ws.datastores:
            print(f"Le Datastore {name} n'est PAS enregistré dans le Workspace {self.ws.name}.")
        else:
            Datastore.get(self.ws, name).unregister()

    def register_csv(self, datastore_name: str, dataset_name: str) -> None:
        if dataset_name in self.ws.compute_targets:
            print(f"Le Dataset {dataset_name} est déjà enregistré dans le Workspace {self.ws.name}.")
        else:
            if datastore_name in self.ws.datastores:
                datastore = Datastore.get(self.ws, datastore_name)
            else:
                raise NameError(f"""Le Datastore {datastore_name} n'est pas enregistré dans le workspace {self.ws.name}
                                    du resource groupe {self.ws.resource_group} de l'id {self.ws.subscription_id}.
                                    Vous pouvez en enregistrer un via le browser web d'AzureML.""")
            if dataset_name.endswith(".csv"):
                dataset_name = dataset_name[:-4]
            path = [(datastore, f"{dataset_name}.csv")]
            data = Dataset.Tabular.from_delimited_files(path=path)
            data.register(workspace=self.ws, name=dataset_name, create_new_version=True)

    def register_compute(self, name: str, compute_size: str = "Standard_NC6", compute_min_nodes: int = 0, compute_max_nodes: int = 1) -> None:
        """Enregistre un compute cluster dans ws.

        Args:
            name (str): Le nom du cluster
            compute_size (str, optional): La taille du cluster. Defaults to "Standard_NC6".
            compute_min_nodes (int, optional): Le nombre minimal de workers. Defaults to 0.
            compute_max_nodes (int, optional): Le nombre maximal de workers. Defaults to 1.
        """
        if name in self.ws.compute_targets:
            print(f"Le Compute {name} est déjà enregistré dans le Workspace {self.ws.name}.")
        else:
            compute_config = AmlCompute.provisioning_configuration(vm_size=compute_size,
                                                                   min_nodes=compute_min_nodes,
                                                                   max_nodes=compute_max_nodes)
            compute = ComputeTarget.create(self.ws, name, compute_config)
            compute.wait_for_completion(show_output=True)
