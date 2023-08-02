import pandas as pd
from pathlib import path

def run(params):
    entrypoint = params.get("entrypoint")
    print(f"Running entrypoint accounts calling for entrypoint: {entrypoint}")