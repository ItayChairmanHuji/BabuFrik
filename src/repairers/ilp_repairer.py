from src.ilp.optimal_data_repair_ilp import OptimalDataRepairILP
from src.ilp.synthetic_data_repair_ilp import SyntheticDataRepairILP
from src.repairers.ilp_repairer_base import ILPRepairerBase


class ILPRepairer(ILPRepairerBase):
    @property
    def ilp_type(self) -> type[OptimalDataRepairILP]:
        return SyntheticDataRepairILP
