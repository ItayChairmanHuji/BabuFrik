import xgboost
from pandas import DataFrame
from sklearn.metrics import accuracy_score


def calculate_quality(private_dataset: DataFrame, synthetic_dataset: DataFrame,
                      repaired_dataset: DataFrame, target_attribute: str) -> float:
    repaired_data_score = get_accuracy(repaired_dataset.drop(columns=[target_attribute]),
                                       repaired_dataset[target_attribute],
                                       private_dataset.drop(columns=[target_attribute]),
                                       private_dataset[target_attribute])
    synthetic_data_score = get_accuracy(synthetic_dataset.drop(columns=[target_attribute]),
                                        synthetic_dataset[target_attribute],
                                        private_dataset.drop(columns=[target_attribute]),
                                        private_dataset[target_attribute])
    return repaired_data_score / synthetic_data_score


def get_accuracy(train_data: DataFrame, train_labels: DataFrame,
                 test_data: DataFrame, test_labels: DataFrame) -> float:
    model = xgboost.XGBClassifier(n_estimators=1000, learning_rate=0.1, max_depth=5)
    model.fit(train_data, train_labels)
    predictions = model.predict(test_data)
    return accuracy_score(test_labels, predictions)
