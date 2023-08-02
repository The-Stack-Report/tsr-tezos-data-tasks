from pathlib import Path
import requests
import json
import os

# 

schemas_dir = Path("schemas")
models_dir = Path("models")
models_base_dir = models_dir / "base"
models_extended_dir = models_dir / "extended"

models = [
    {
        "name": "Dataset",
        "path": "dataset.py",
        "class": "Dataset",
        "source_schema_url": "https://raw.githubusercontent.com/The-Stack-Report/tsr-resource-descriptions/main/resources/dataset.schema.json"
    },
]

if __name__ == "__main__":
    schemas_dir.mkdir(exist_ok=True)
    models_dir.mkdir(exist_ok=True)
    models_base_dir.mkdir(exist_ok=True)
    models_extended_dir.mkdir(exist_ok=True)

    for model in models:
        model_path = models_dir / model["path"]
        schema_url =  model["source_schema_url"]
        schema_path = schemas_dir / Path(schema_url).name
        schema_path.touch(exist_ok=True)

        base_model_path = models_base_dir / model["path"]
        # Fetch schema url and write to schema_path
        schema_json = requests.get(schema_url).json()
        with open(schema_path, "w") as f:
            json.dump(schema_json, f, indent=4)
        

        extended_model_path = models_extended_dir / model["path"]
        extended_model_path.touch(exist_ok=True)
        
        os.system(f"json-schema-to-class {schema_path} -o {base_model_path}")
        # os.system(f"json-schema-to-class {schema_path} {model_path} --class-name {model['class']} --base-class-name BaseModel --extended-class-name ExtendedModel")


        model_path.touch(exist_ok=True)
