from Cscorer.data.factory import get_data
from Cscorer.core import read_config, PipelineData
from pathlib import Path
import argparse
import subprocess

def main():
    
    parser = argparse.ArgumentParser(
                    prog='BioCity',
                    description='What the program does',
                    epilog='Text at the bottom of help'
    )
    
    parser.add_argument("--file", "-f",help = 'Config File')
    parser.add_argument("--dev", "-d", action= 'store_true', help = 'Run dev')
    parser.add_argument("--debug", action= 'store_true', help = 'Run debugger')

    args = parser.parse_args()
    
    if not args.file:
        if not args.dev:
            raise UserWarning("Missing config file")
        config = Path(__file__).parent.parent.parent / "work/dev/config.yaml"
        print(config)