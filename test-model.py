  # This is the label column for classification
import requests
import json

BASE_URL = "http://localhost:8000/api/models/train"

file_id = "3dd462c95-c5aa-4f3b-b2e9-cf0242c1cf18"
target_column = "species"

def test_random_forest():
    payload = {
        "file_id": file_id,
        "target_column": target_column,
        "model_type": "random_forest",
        "parameters": {
            "n_estimators": 100,
            "max_depth": 5
        }
    }
    response = requests.post(BASE_URL, json=payload)
    result = response.json()
    print("Random Forest Model Response:")
    print(json.dumps(result, indent=2))

def test_knn():
    payload = {
        "file_id": file_id,
        "target_column": target_column,
        "model_type": "knn",
        "parameters": {
            "n_neighbors": 3
        }
    }
    response = requests.post(BASE_URL, json=payload)
    result = response.json()
    print("kNN Model Response:")
    print(json.dumps(result, indent=2))


def test_adaboost():
    payload = {
        "file_id": file_id,
        "target_column": target_column,
        "model_type": "adaboost",
        "parameters": {
            "n_estimators": 50
        }
    }
    response = requests.post(BASE_URL, json=payload)
    result = response.json()
    print("AdaBoost Model Response:")
    print(json.dumps(result, indent=2))

def test_naive_bayes():
    payload = {
        "file_id": file_id,
        "target_column": target_column,
        "model_type": "naive_bayes"
    }
    response = requests.post(BASE_URL, json=payload)
    result = response.json()
    print("Naive Bayes Model Response:")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    test_random_forest()
    test_knn()
    test_adaboost()
    test_naive_bayes()
