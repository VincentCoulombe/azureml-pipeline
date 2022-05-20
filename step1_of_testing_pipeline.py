from azureml_wrapper import ScriptWrapper


script = ScriptWrapper()
config = script.get_config()
new_df = script.get_csv_from_config(config.get("new_dataset_name"))
script.save_csv_in_output_folder(new_df, config.get("new_dataset_name"))
script.run.complete()
