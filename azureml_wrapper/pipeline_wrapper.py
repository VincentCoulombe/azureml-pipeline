from __future__ import annotations
from typing import Any, Dict, List, Union, Optional

from azureml.core import Datastore, Environment, Experiment
from azureml.data import OutputFileDatasetConfig
from azureml.pipeline.steps import PythonScriptStep
from azureml.pipeline.core import Pipeline, Schedule, ScheduleRecurrence
from azureml.core.compute import ComputeTarget
from azureml.core.compute_target import ComputeTargetException
from azureml.core.runconfig import RunConfiguration

from .workspace_wrapper import WorkspaceWrapper
from .pipeline_step import PipelineStep


class PipelineWrapper(WorkspaceWrapper):
    MANDATORY_CONFIGS = ["ws_name", "resource_group", "subscription_id", "env_name", "compute_name", "steps"]
    POSSIBLE_SCHEDULES = ["On_blob_change", "Minute", "Hour", "Day", "Week", "Month"]

    def __init__(self, ws_name: str, resource_group: str, subscription_id: str, env_name: str, compute_name: str,
                 steps: List[PipelineStep]) -> None:

        """Wrap autour des la mécanique des Pipelines du AzureML sdk afin d'éviter à avoir à refaire la poutine à toutes les fois.
            Simplement spécifier les différents nom et entrer une liste contenant votre ou vos steps. ATTENTION, un OutputFileDatasetConfig
            est automatiquement passé entre vos steps (si plus qu'un) et il est accessible via les arguments --input-folder et --output-folder.
            Pour plus de détails, voir la documentation.

        Args:
            ws_name (str): Le nom du Workspace
            resource_group (str): Le nom du Resource Group
            subscription_id (str): L'id de l'utilisateur
            env_name (str): Le nom de l'Environment (doit être enregistré dans le Workspace ws_name)
            compute_name (str): Le nom du ComputeTarget.
            steps (list): Liste des PipelineStep du Pipeline

        """
        if not isinstance(steps, list):
            raise TypeError("steps doit être une liste, même si elle ne contient qu'un step.")
        super().__init__(ws_name, resource_group, subscription_id)
        self.run_config = RunConfiguration()
        self.run_config.environment = Environment.get(self.ws, env_name)
        try:
            self.compute = ComputeTarget(workspace=self.ws, name=compute_name)
        except ComputeTargetException as e:
            raise TypeError(f"""Le compute {compute_name} n'est pas enregistré dans le Workspace {self.ws.name}.
                            Vous pouvez le faire via la méthode register_compute().""") from e
        self._run: Optional[Experiment] = None
        self.run_config.target = self.compute
        self.steps = steps
        self.pipeline_steps: List[PythonScriptStep] = []
        self.folder = OutputFileDatasetConfig()
        for step in self.steps:
            if not isinstance(step, PipelineStep):
                raise TypeError("Le paramètre step doit être un instance de la classe PipelineStep.")
            if self.ws.name != step.ws.name:
                raise ValueError(f"Le Workspace du paramètre step ({step.ws.name}) doit être le même que celui du pipeline ({self.ws.name})")
            if step != self.steps[-1]:
                step.arguments.extend(["--output-folder", self.folder])
            if step != self.steps[0]:
                step.arguments.extend(["--input-folder", self.folder.as_input()])
                self.folder = OutputFileDatasetConfig()
            self.pipeline_steps.append(PythonScriptStep(name=step.name,
                                                        source_directory=step.script_directory,
                                                        script_name=step.script_name,
                                                        arguments=step.arguments,
                                                        compute_target=self.compute,
                                                        runconfig=self.run_config,
                                                        allow_reuse=True))

    @property
    def pipeline(self) -> Pipeline:
        return self._pipeline

    @pipeline.setter
    def pipeline(self, new_pipeline: Pipeline) -> None:
        if isinstance(new_pipeline, Pipeline):
            self._pipeline = new_pipeline

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> PipelineWrapper:
        if not isinstance(config, Dict):
            raise TypeError("config doit être un dict.")
        missing_keys = [key for key in cls.MANDATORY_CONFIGS if key not in config]
        if missing_keys:
            raise KeyError(f"Votre configuration doit contenir la (ou les) clée(s) suivante(s) : {missing_keys} pour être valide.")
        base_config = {"ws_name": config.get("ws_name"), "resource_group": config.get("resource_group"), "subscription_id": config.get("subscription_id")}
        steps_config = config.get("steps")
        steps: List[PipelineStep] = []
        if steps_config is not None:
            for _, step_config in steps_config.items():
                step_config = {**base_config, **step_config}
                steps.append(PipelineStep.from_config(step_config))
        return cls(str(config.get("ws_name")), str(config.get("resource_group")), str(config.get("subscription_id")), str(config.get("env_name")),
                   str(config.get("compute_name")), steps)

    def run(self, experiment_name: str) -> None:
        if len(self.pipeline_steps) > 0:
            self.pipeline = Pipeline(workspace=self.ws, steps=self.pipeline_steps)
            self.experiment = Experiment(workspace=self.ws, name=experiment_name)
            self._run = self.experiment.submit(self.pipeline)
            self._run.wait_for_completion()

    def register(self, name: str, description: str, schedule: Union[str, None] = None, interval: Union[int, None] = None,
                 datastore_name: Union[str, None] = None) -> None:
        if self._run is None:
            raise ValueError("Vous devez voir lancer un expérience pour pouvoir la publiée. Vous pouvez le faire via .run().")
        self.pipeline = self._run.publish_pipeline(name=name, description=description, version="0")
        if schedule:
            if schedule not in self.POSSIBLE_SCHEDULES:
                raise ValueError(f"Les schédules possibles sont : {self.POSSIBLE_SCHEDULES}.")
            if schedule == "On_blob_change":
                if not datastore_name:
                    raise ValueError(f"Avec la schédule {schedule}, vous devez préciser un datastore_name vers le blob en question.")
                datastore = Datastore.get(workspace=self.ws, name=datastore_name)
                schedule = Schedule.create(self.ws, name=f"{name}Schedule", pipeline_id=self.pipeline.id,
                                           experiment_name=self.experiment.name, datastore=datastore)
            else:
                if not interval:
                    raise ValueError(f"Avec la schédule {schedule}, un interval de {schedule}.")
                recurrence = ScheduleRecurrence(frequency=schedule, interval=interval)
                schedule = Schedule.create(self.ws, name=f"{name}Schedule",
                                           description=description,
                                           pipeline_id=self.pipeline.id,
                                           experiment_name=self.experiment.name,
                                           recurrence=recurrence)
