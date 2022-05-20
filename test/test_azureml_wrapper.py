from azureml.core import Workspace
from typing import Dict
import pytest
import json

from azureml_wrapper import PipelineStep, PipelineWrapper, WorkspaceWrapper


@pytest.fixture(scope="session")
def config():

    with open(r"test\workspaceConfig.json") as config_file:
        return json.load(config_file)


@pytest.fixture(scope="session")
def ws_wrapper(config: Dict[str, str]):

    return WorkspaceWrapper.from_config(config)


@pytest.fixture(scope="session")
def pipeline_wrapper(config: Dict[str, str], ws_wrapper: WorkspaceWrapper):

    if "test-compute00001" not in ws_wrapper.ws.compute_targets:
        ws_wrapper.register_compute("test-compute00001")
    if "test-env" not in ws_wrapper.ws.compute_targets:
        ws_wrapper.register_env("test-env", ["git+https://github.com/VincentCoulombe/azureml_wrapper.git",
                                             "azureml.core", "numpy", "pandas", "argparse"])
    if "test_ds" not in ws_wrapper.ws.datastores:
        ws_wrapper.register_blob_datastore("test_ds", str(config.get("test_container")), str(config.get("storage_acc_name")),
                                           str(config.get("storage_key")))
    if "test" not in ws_wrapper.ws.datasets:
        ws_wrapper.register_csv("test_ds", "test")
    step_config = {**config,
                   "env_name": "test-env",
                   "compute_name": "test-compute00001",
                   "steps": {"step1": {"step_name": "test step1",
                                       "script_name": "step1_of_testing_pipeline.py",
                                       "step_config": {"new_dataset_name": "test", "min_text_lenght": 5},
                                       "input_datasets": {"--new-data": "test"}},
                             "step2": {"step_name": "test step2",
                                       "script_name": "step2_of_testing_pipeline.py",
                                       "step_config": {"new_dataset_name": "test", "epochs": 1, "batch_size": 16}}}}
    return PipelineWrapper.from_config(step_config)


def test_init_WorkspaceWrapper(config: Dict[str, str], ws_wrapper: WorkspaceWrapper):

    ws_name, resource_group, subscription_id = str(config.get("ws_name")), str(config.get("resource_group")), str(config.get("subscription_id"))
    assert(isinstance(ws_wrapper.ws, Workspace))
    assert(ws_name == ws_wrapper.ws.name)
    assert(resource_group == ws_wrapper.ws.resource_group)
    assert(subscription_id == ws_wrapper.ws.subscription_id)


def test_register_blob_datastore_WorkspaceWrapper(config: Dict[str, str], ws_wrapper: WorkspaceWrapper):

    ws_wrapper.unregister_blob_datastore("test_ds")
    assert("test_ds" not in ws_wrapper.ws.datastores)
    ws_wrapper.register_blob_datastore("test_ds", str(config.get("test_container")), str(config.get("storage_acc_name")),
                                       str(config.get("storage_key")))
    if "test" not in ws_wrapper.ws.datasets:
        ws_wrapper.register_csv("test_ds", "test")
    assert("test_ds" in ws_wrapper.ws.datastores)
    ws_wrapper.unregister_blob_datastore("test_ds")
    assert("test_ds" not in ws_wrapper.ws.datastores)


def test_init_PipelineStep(config: Dict[str, str]):

    step_config = {**config, "step_name": "test step1",
                             "script_name": "test_step1.py",
                             "step_config": {"lr": 0.00002,
                                             "nb_epochs": 10,
                                             "batch_size": 16},
                             "input_datasets": {"--new-data": "test"}}
    step = PipelineStep.from_config(step_config)
    script_config = {"lr": 0.00002, "nb_epochs": 10, "batch_size": 16}
    arguments = ["--config", json.dumps(script_config), "--new-data"]
    assert(step.arguments[:3] == arguments)
    assert(step.script_name == "test_step1.py")
    assert(step.name == "test step1")
    with pytest.raises(NameError):
        del step_config["input_datasets"]
        step_config["input_datasets"] = {"--new-data": "bad_name"}
        step = PipelineStep.from_config(step_config)
    with pytest.raises(TypeError):
        step = PipelineStep.from_config([])
    with pytest.raises(KeyError):
        del step_config["step_name"]
        step = PipelineStep.from_config(step_config)


def test_init_PipelineWrapper(pipeline_wrapper: PipelineWrapper):

    assert(len(pipeline_wrapper.steps) == 2)
    for step in [0, 1]:
        step_name = pipeline_wrapper.steps[step].name
        script_name = pipeline_wrapper.steps[step].script_name
        step_args = pipeline_wrapper.steps[step].arguments
        if step == 0:
            assert(step_name == "test step1")
            assert(script_name == "step1_of_testing_pipeline.py")
            assert("--output-folder" in step_args and "--input-folder" not in step_args)
        elif step == 1:
            assert(step_name == "test step2")
            assert(script_name == "step2_of_testing_pipeline.py")
            assert("--input-folder" in step_args and "--output-folder" not in step_args)


def test_run_PipelineWrapper(pipeline_wrapper: PipelineWrapper, ws_wrapper: WorkspaceWrapper):

    pipeline_wrapper.run("test_run")
    assert(pipeline_wrapper.experiment.name == "test_run")
    assert(pipeline_wrapper.experiment.workspace.name == ws_wrapper.ws.name)
    assert(pipeline_wrapper._run.experiment.name == pipeline_wrapper.experiment.name)


def test_register_PipelineWrapper(config: Dict[str, str], ws_wrapper: WorkspaceWrapper):
    pass
