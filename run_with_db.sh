#!/bin/bash
#SBATCH --time=24:00:00
#SBATCH --mem=150G
#SBATCH --gres=gpu:0
#SBATCH --cpus-per-task=7
#SBATCH --output=output_%j.log
#SBATCH --error=error_%j.log

# Your commands here
. .venv/bin/activate
pip install -r requirements.txt
python3 -u ./main.py $1
