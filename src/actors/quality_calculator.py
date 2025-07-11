from src.entities.algorithms import QualityFunctions
from src.entities.task import Task
from src.quality_functions import violations_count_quality_function


def calculate_quality(task: Task, quality_function: QualityFunctions, target_attribute: str) -> float:
    match quality_function:
        case QualityFunctions.NO_QUALITY:
            return 0
        case QualityFunctions.VIOLATIONS_COUNT:
            return violations_count_quality_function.calculate_quality(synthetic_dataset=task.synthetic_data,
                                                                       fds=task.fds)
        # case QualityFunctions.DELETION_OVERHEAD:
        #     return deletion_overhead_quality_function.calculate_quality(synthetic_dataset=task.synthetic_data,
        #                                                                 repaired_dataset=task.repaired_data,
        #                                                                 marginals=task.marginals, fds=task.fds,
        #                                                                 marginals_error_margins=marginals_error_margins)
        # case QualityFunctions.MARGINALS_DISTANCE:
        #     return marginals_distance_quality_function.calculate_quality(repaired_dataset=task.repaired_data,
        #                                                                  marginals=task.marginals)
        #
        # case QualityFunctions.ML_ACCURACY:
        #     return machine_learning_accuracy_quality_function.calculate_quality(private_dataset=task.private_data,
        #                                                                         synthetic_dataset=task.repaired_data,
        #                                                                         repaired_dataset=task.repaired_data,
        #                                                                         target_attribute=target_attribute)
