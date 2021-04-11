import numpy as np
import pandas as pd
import glob, os


from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import model_selection, naive_bayes, svm
from sklearn.preprocessing import LabelEncoder

from sklearn.metrics import accuracy_score
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score
from sklearn.metrics import f1_score

import time


pathforcsv=r'D:\Semester1(Fast)\MathsandStatistics\Project\__MACOSX'

start = time.time()
preprocessedata = os.path.join(pathforcsv,r'test1.xlsx')

Corpus = pd.read_excel(preprocessedata,sheetname=0,header=0,index_col=False,keep_default_na=True)

print(Corpus)

Train_X, Test_X, Train_Y, Test_Y = model_selection.train_test_split(Corpus['Total'],Corpus['year'],test_size=0.3)

Encoder = LabelEncoder()
Train_Y = Encoder.fit_transform(Train_Y)
Test_Y = Encoder.fit_transform(Test_Y)


Tfidf_vect = TfidfVectorizer(max_features=5000)
Tfidf_vect.fit(Corpus['Total'])
Train_X_Tfidf = Tfidf_vect.transform(Train_X)
Test_X_Tfidf = Tfidf_vect.transform(Test_X)


Naive = naive_bayes.MultinomialNB()
Naive.fit(Train_X_Tfidf,Train_Y)
# # # predict the labels on validation dataset
predictions_NB = Naive.predict(Test_X_Tfidf)
# # # Use accuracy_score function to get the accuracy


end = time.time()

elapsed = end - start

print( "\n")

print("Time Elapsed = ", elapsed)

print( "\n")

print("Naive Bayes Accuracy Score -> ",accuracy_score(predictions_NB, Test_Y)*100)

print("Naive Bayes Recall Score -> ",recall_score(predictions_NB, Test_Y,average='weighted')*100)

print("Naive Bayes F1 Score -> ",f1_score(predictions_NB, Test_Y,average='weighted')*100)

print("Naive Bayes Precision Score -> ",precision_score(predictions_NB, Test_Y,average='weighted')*100)
			  