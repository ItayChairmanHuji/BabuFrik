#!/bin/bash
#SBATCH --time=20:00:00
#SBATCH --mem=100G
#SBATCH --gres=gpu:1
#SBATCH --cpus-per-task=4
#SBATCH --output=output_%j.log
#SBATCH --error=error_%j.log

# Your commands here
. .venv/bin/activate
python3 -u ./main.py $1
