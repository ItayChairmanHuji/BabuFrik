sbatch ./run.sh patectgan_private.json 
sbatch ./run.sh patectgan_synthetic.json
sbatch ./run.sh patectgan_constraints.json

sbatch ./run.sh mst_private.json
sbatch ./run.sh mst_synthetic.json
sbatch ./run.sh mst_constraints.json

sbatch ./run.sh patectgan_ilp_synthetic.json
sbatch ./run.sh patectgan_ilp_constraints.json

ssbatch ./run.sh mst_ilp_synthetic.json
sbatch ./run.sh mst_ilp_constraints.json

sbatch ./run.sh patectgan_vertex_cover_synthetic.json
sbatch ./run.sh patectgan_vertex_cover_constraints.json

ssbatch ./run.sh mst_vertex_cover_synthetic.json
sbatch ./run.sh mst_vertex_cover_constraints.json

sbatch ./run.sh patectgan_greedy_synthetic.json
sbatch ./run.sh patectgan_greedy_constraints.json

ssbatch ./run.sh mst_greedy_synthetic.json
sbatch ./run.sh mst_greedy_constraints.json