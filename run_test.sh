#!/bin/bash
#SBATCH --time=1:00:00
#SBATCH --mem=20G
#SBATCH --cpus-per-task=3
#SBATCH --output=output_%j.log
#SBATCH --error=error_%j.log

# Your commands here
. .venv/bin/activate
pip install -r requirements.txt
python3 -u ./main.py $1
