from src.utils.ilp_model import ILPModel
from src.marginals.marginals import Marginals


class WeightFunctionAdder:
    @staticmethod
    def add_weight_function(ilp: ILPModel) -> ILPModel:
        weights = {i: WeightFunctionAdder.__get_tuple_weight(ilp, i) for i in range(len(ilp.data))}
        ilp.weight_function = lambda i: weights[i]
        return ilp

    @staticmethod
    def __get_tuple_weight(ilp: ILPModel, tuple_index: int) -> float:
        data_without_tuple = ilp.data.drop(index=tuple_index)
        marginals_without_tuple = Marginals(data_without_tuple)
        return ilp.marginals.distance(marginals_without_tuple)
