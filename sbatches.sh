sbatch ./run.sh synthesizers/mst/private/adult.json
sbatch ./run.sh synthesizers/mst/private/flight.json
sbatch ./run.sh synthesizers/mst/private/compas.json

sbatch ./run.sh synthesizers/mst/synthetic/adult.json
sbatch ./run.sh synthesizers/mst/synthetic/flight.json
sbatch ./run.sh synthesizers/mst/synthetic/compas.json

sbatch ./run.sh synthesizers/patectgan/private/adult.json
sbatch ./run.sh synthesizers/patectgan/private/flight.json
sbatch ./run.sh synthesizers/patectgan/private/compas.json

sbatch ./run.sh synthesizers/patectgan/synthetic/adult.json
sbatch ./run.sh synthesizers/patectgan/synthetic/flight.json
sbatch ./run.sh synthesizers/patectgan/synthetic/compas.json

sbatch ./run.sh repairs/greedy/private/adult_mst.json
sbatch ./run.sh repairs/greedy/private/adult_patectgan.json
sbatch ./run.sh repairs/greedy/private/flight_mst.json
sbatch ./run.sh repairs/greedy/private/flight_patectgan.json
sbatch ./run.sh repairs/greedy/private/compas_mst.json
sbatch ./run.sh repairs/greedy/private/compas_patectgan.json

sbatch ./run.sh repairs/greedy/synthetic/adult_mst.json
sbatch ./run.sh repairs/greedy/synthetic/adult_patectgan.json
sbatch ./run.sh repairs/greedy/synthetic/flight_mst.json
sbatch ./run.sh repairs/greedy/synthetic/flight_patectgan.json
sbatch ./run.sh repairs/greedy/synthetic/compas_mst.json
sbatch ./run.sh repairs/greedy/synthetic/compas_patectgan.json

sbatch ./run.sh repairs/ilp/private/adult_mst.json
sbatch ./run.sh repairs/ilp/private/adult_patectgan.json
sbatch ./run.sh repairs/ilp/private/flight_mst.json
sbatch ./run.sh repairs/ilp/private/flight_patectgan.json
sbatch ./run.sh repairs/ilp/private/compas_mst.json
sbatch ./run.sh repairs/ilp/private/compas_patectgan.json

sbatch ./run.sh repairs/ilp/synthetic/adult_mst.json
sbatch ./run.sh repairs/ilp/synthetic/adult_patectgan.json
sbatch ./run.sh repairs/ilp/synthetic/flight_mst.json
sbatch ./run.sh repairs/ilp/synthetic/flight_patectgan.json
sbatch ./run.sh repairs/ilp/synthetic/compas_mst.json
sbatch ./run.sh repairs/ilp/synthetic/compas_patectgan.json

sbatch ./run.sh repairs/vertex_cover/private/adult_mst.json
sbatch ./run.sh repairs/vertex_cover/private/adult_patectgan.json
sbatch ./run.sh repairs/vertex_cover/private/flight_mst.json
sbatch ./run.sh repairs/vertex_cover/private/flight_patectgan.json
sbatch ./run.sh repairs/vertex_cover/private/compas_mst.json
sbatch ./run.sh repairs/vertex_cover/private/compas_patectgan.json

sbatch ./run.sh repairs/vertex_cover/synthetic/adult_mst.json
sbatch ./run.sh repairs/vertex_cover/synthetic/adult_patectgan.json
sbatch ./run.sh repairs/vertex_cover/synthetic/flight_mst.json
sbatch ./run.sh repairs/vertex_cover/synthetic/flight_patectgan.json
sbatch ./run.sh repairs/vertex_cover/synthetic/compas_mst.json
sbatch ./run.sh repairs/vertex_cover/synthetic/compas_patectgan.json