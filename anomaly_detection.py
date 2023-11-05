
"""
Step 1: training:
- read the no-anomaly csv file into pandas df
- import sklearn isolation forests
- fit the isolation forest to the no-anomaly dataset
- pickle the classifier
"""
import pandas as pd
import numpy as np
import pickle
from sklearn.ensemble import IsolationForest


def step1_training():
    train = pd.read_csv("./anomaly-detection-data/artificialNoAnomaly/artificialNoAnomaly/art_daily_small_noise.csv")
    clf = IsolationForest(max_samples=100, random_state=0)
    # print(train.loc[:, ["value"]].head())
    clf.fit(train.loc[:, ["value"]].values)
    filename = 'clf.pkl'
    pickle.dump(clf, open(filename, 'wb'))
       

"""
Step 2: inference:
- unpickle the classifier
- read in the with-anomaly csv into pandas df
- row by row, run classifier.predict(row)
- print the output to a log
"""

def step2_inference():
    # load the model from disk
    filename = 'clf.pkl'
    loaded_model = pickle.load(open(filename, 'rb'))
    result = loaded_model.predict([[80.2149468852]])
    print(result)


if __name__ == "__main__":
    step1_training()
    # step2_inference()