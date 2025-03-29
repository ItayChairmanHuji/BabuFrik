from src.ilp.optimal_data_repair_ilp import OptimalDataRepairILP
from src.ilp.vertex_cover_data_repair_ilp import VertexCoverDataRepairILP
from src.repairers.ilp_repairer_base import ILPRepairerBase


class VertexCoverRepairer(ILPRepairerBase):
    @property
    def ilp_type(self) -> type[OptimalDataRepairILP]:
        return VertexCoverDataRepairILP
