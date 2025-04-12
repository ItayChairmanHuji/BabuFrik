sbatch ./run.sh synthesizers/mst/private/adult_test.json
sbatch ./run.sh synthesizers/mst/private/flight_test.json
sbatch ./run.sh synthesizers/mst/private/compas_test.json

sbatch ./run.sh synthesizers/mst/synthetic/adult_test.json
sbatch ./run.sh synthesizers/mst/synthetic/flight_test.json
sbatch ./run.sh synthesizers/mst/synthetic/compas_test.json

sbatch ./run.sh synthesizers/patectgan/private/adult_test.json
sbatch ./run.sh synthesizers/patectgan/private/flight_test.json
sbatch ./run.sh synthesizers/patectgan/private/compas_test.json

sbatch ./run.sh synthesizers/patectgan/synthetic/adult_test.json
sbatch ./run.sh synthesizers/patectgan/synthetic/flight_test.json
sbatch ./run.sh synthesizers/patectgan/synthetic/compas_test.json

sbatch ./run.sh repairs/greedy/private/adult_mst_test.json
sbatch ./run.sh repairs/greedy/private/adult_patectgan_test.json
sbatch ./run.sh repairs/greedy/private/flight_mst_test.json
sbatch ./run.sh repairs/greedy/private/flight_patectgan_test.json
sbatch ./run.sh repairs/greedy/private/compas_mst_test.json
sbatch ./run.sh repairs/greedy/private/compas_patectgan_test.json

sbatch ./run.sh repairs/greedy/synthetic/adult_mst_test.json
sbatch ./run.sh repairs/greedy/synthetic/adult_patectgan_test.json
sbatch ./run.sh repairs/greedy/synthetic/flight_mst_test.json
sbatch ./run.sh repairs/greedy/synthetic/flight_patectgan_test.json
sbatch ./run.sh repairs/greedy/synthetic/compas_mst_test.json
sbatch ./run.sh repairs/greedy/synthetic/compas_patectgan_test.json

sbatch ./run.sh repairs/ilp/private/adult_mst_test.json
sbatch ./run.sh repairs/ilp/private/adult_patectgan_test.json
sbatch ./run.sh repairs/ilp/private/flight_mst_test.json
sbatch ./run.sh repairs/ilp/private/flight_patectgan_test.json
sbatch ./run.sh repairs/ilp/private/compas_mst_test.json
sbatch ./run.sh repairs/ilp/private/compas_patectgan_test.json

sbatch ./run.sh repairs/ilp/synthetic/adult_mst_test.json
sbatch ./run.sh repairs/ilp/synthetic/adult_patectgan_test.json
sbatch ./run.sh repairs/ilp/synthetic/flight_mst_test.json
sbatch ./run.sh repairs/ilp/synthetic/flight_patectgan_test.json
sbatch ./run.sh repairs/ilp/synthetic/compas_mst_test.json
sbatch ./run.sh repairs/ilp/synthetic/compas_patectgan_test.json

sbatch ./run.sh repairs/vertex_cover/private/adult_mst_test.json
sbatch ./run.sh repairs/vertex_cover/private/adult_patectgan_test.json
sbatch ./run.sh repairs/vertex_cover/private/flight_mst_test.json
sbatch ./run.sh repairs/vertex_cover/private/flight_patectgan_test.json
sbatch ./run.sh repairs/vertex_cover/private/compas_mst_test.json
sbatch ./run.sh repairs/vertex_cover/private/compas_patectgan_test.json

sbatch ./run.sh repairs/vertex_cover/synthetic/adult_mst_test.json
sbatch ./run.sh repairs/vertex_cover/synthetic/adult_patectgan_test.json
sbatch ./run.sh repairs/vertex_cover/synthetic/flight_mst_test.json
sbatch ./run.sh repairs/vertex_cover/synthetic/flight_patectgan_test.json
sbatch ./run.sh repairs/vertex_cover/synthetic/compas_mst_test.json
sbatch ./run.sh repairs/vertex_cover/synthetic/compas_patectgan_test.json