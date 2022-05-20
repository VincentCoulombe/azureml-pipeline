from azureml_wrapper import ScriptWrapper


script = ScriptWrapper()
config = script.get_config()
test_df = script.get_csv_from_input_folder(config.get("new_dataset_name"))
script.run.complete()
