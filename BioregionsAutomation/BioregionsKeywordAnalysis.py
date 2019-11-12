########################################################################################################################

# Title: Keyword Analysis
# Authors: Matear, L.(2019)                                                               Email: Liam.Matear@jncc.gov.uk
# Version Control: 1.0

# Script description:    This Python script is designed for use with the Bioregions Automation process. Data inputs,
#                        such as the Bioregions DB are used to provide evidence for habitat distribution. Due to the
#                        nature or work completed by various individuals over time, data stored within this DB does not
#                        follow a standardised method, nor vocabulary. Therefore, this script aims to alleviate the
#                        issues associated with non-standardisation through automated keyword identification through
#                        machine learning.
#
#                        To ensure no permanent alterations are made to the master documents, all data used within this
#                        script are copies of the original files.
#
#                        For any enquiries please contact Liam Matear by email: Liam.Matear@jncc.gov.uk

########################################################################################################################

#                                                Introduction & Setup                                                  #

########################################################################################################################

# Section 1: Introduction
# For visual overview, see 'Methodology Infographic' file within the 'Bioregions Automation' folder

# Process Outline:

# Import all libraries required for code execution within the IDE
# Import libraries for data manipulation
import os
import re
import time
import numpy as np
import pandas as pd


# Data exploration
from os import path
from PIL import Image
import seaborn as sns
import matplotlib.pyplot as plt
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import RegexpTokenizer
from nltk.stem.wordnet import WordNetLemmatizer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer



########################################################################################################################

#                                             1. Data import and cleaning                                              #

########################################################################################################################


#         Import copy of biotopes database
Biotopes_DB = pd.read_excel(r"Z:\Marine\Evidence\PressuresImpacts\6. Sensitivity\SA's Contracts\C16-0257-105 Biogeographical Regional Contract\Report\Final\Biotope database_Final_270217.xlsx", 'Biotope Database')


########################################################################################################################

#                                        1. Text cleaning and data scrubbing                                           #

########################################################################################################################

# Identify the most commonly used words within each column
word_freq = pd.Series(' '.join(Biotopes_DB['1. Northern North Sea '].astype(str)).split()).value_counts()

# Convert the Pandas Series Object into a DataFrame to be manipulated later in the script
counts_to_stopwords = pd.DataFrame(word_freq)

# Reset the index of the DF to rename the columns appropriately
counts_to_stopwords = counts_to_stopwords.reset_index(inplace=False)

# Rename the columns as desired
counts_to_stopwords.columns = ['Word', 'Count']

# Pull out all unique words to select which words should be culled as a stopword
unique_words = list(counts_to_stopwords['Word'].unique())

# Develop a set type object to store all stopwords to be removed from the analysis
stop_words = set()

# Develop a list of custom stopwords to be removed
cull_words = [
    'and', 'and', 'jncc', 'biotope', 'description', 'contour', 'contour)', 'based', 'on', 'the', 'this'

    # 'and', 'and', 'the', 'this', 'as', 'be', 'a', 'by', 'with', 'been', 'very', 'are', 'contour'
    # 'from', '#NAME?', 'both', 'up', 'an', 'gases', '(â‰¥', 'at', 'E', 'while', 'although', 'annd',
    # 'Agree', 'ansd', 'these', 'along', 'The', 'adjacent', 'baed', 'contour)', 'of',
    # 'to', 'in',  'support', 'is', 'part', 'assigned', 'general', 'thin', 'that',
    # 'similar', 'levels', 'more', 'oil', 'outside', 'may', 'categorised', 'supplied',
    # 'scour', 'project', 'level', 'so',  'due',  'but',  'numbers',  'table',  'requirement',  'large',
    # "gases'",  'Out',  'majority', 'north',  'note',  'soft',  'made', 'shows',  'layer', 'It',  'three',
    # 'northern', 'same', 'who', 'most', 'thorughout', 'map', 'it', 'centre', 'agree', 'jncc', 'name'
]


# Add custom stopwords to the stopwords list
stop_words = stop_words.union(cull_words)

# Develop text corpus and clean text data of unwanted elements
corpus = []
for i in range(0, 294):
    # Remove punctuations
    text = re.sub('[^a-zA-Z]', ' ', Biotopes_DB['1. Northern North Sea '].astype(str)[i])

    # Convert to lowercase
    text = text.lower()

    # remove tags
    text = re.sub("&lt;/?.*?&gt;", " &lt;&gt; ", text)

    # remove special characters and digits
    text = re.sub("(\\d|\\W)+", " ", text)

    # Convert to list from string
    text = text.split()

    # Stemming
    ps = PorterStemmer()
    # Lemmatisation
    lem = WordNetLemmatizer()
    text = [lem.lemmatize(word) for word in text if word not in stop_words]
    text = " ".join(text)
    corpus.append(text)


########################################################################################################################

#                                        2. Text exploration: Visualisation                                            #

########################################################################################################################


# Create wordcloud to visualise the most common words which occur within the descriptions
wordcloud = WordCloud(
                          background_color='white',
                          stopwords=stop_words,
                          max_words=100,
                          max_font_size=40,
                          random_state=42
                         ).generate(str(corpus))

fig = plt.figure(1)
plt.imshow(wordcloud)
plt.axis('off')
plt.show()


########################################################################################################################

#                                   3. Text preparation: Tokenisation and Vectorisation                                #

########################################################################################################################

# Prior to running the machine learning algorithms, the text must be converted into two formats which will facilitate
# further analysis.

# The two key parts of this process include Tokenisation and Vectorisation.
# To complete this process of text preparation, we utilise the bag of words model, a technique which ignores the
# sequence of words, and only accounts or the frequencies of occurrence


# Creating a vector of word counts
#
# # Utilise the sklearn CountVectorizer to tokenise the text and develop a vocabulary of known words.
# cv = CountVectorizer(
#     max_df=1,  # Ignore terms with a document frequency above this threshold (corpus specific words) - not sure if we
#                  # want this?
#     stop_words=stop_words,
#     max_features=10000,  # Maximum columns within the matrix
#     ngram_range=(1, 3))  # Determines the list of words - single, bi-gram and tri-gram word combinations
#
# # Utilise the fit_transform function to learn and develop the library
# X = cv.fit_transform(corpus)

# Examine an encoded vector of vocabulary
list(cv.vocabulary_.keys())[:10]


########################################################################################################################

#                               4. Data visualisation: N uni-grams, bi-grams and tri-grams                             #

########################################################################################################################


#Most frequently occuring words
def get_top_n_words(corpus, n=None):
    vec = CountVectorizer().fit(corpus)
    bag_of_words = vec.transform(corpus)
    sum_words = bag_of_words.sum(axis=0)
    words_freq = [(word, sum_words[0, idx]) for word, idx in
                   vec.vocabulary_.items()]
    words_freq =sorted(words_freq, key = lambda x: x[1],
                       reverse=True)
    return words_freq[:n]


#Convert most freq words to dataframe for plotting bar plot
top_words = get_top_n_words(corpus, n=20)
top_df = pd.DataFrame(top_words)
top_df.columns=["Word", "Freq"]


#Barplot of most freq words
sns.set(rc={'figure.figsize':(13,8)})
g = sns.barplot(x="Word", y="Freq", data=top_df)
g.set_xticklabels(g.get_xticklabels(), rotation=40)



#Most frequently occuring Bi-grams
def get_top_n2_words(corpus, n=None):
    vec1 = CountVectorizer(ngram_range=(2,2),
            max_features=2000).fit(corpus)
    bag_of_words = vec1.transform(corpus)
    sum_words = bag_of_words.sum(axis=0)
    words_freq = [(word, sum_words[0, idx]) for word, idx in
                  vec1.vocabulary_.items()]
    words_freq =sorted(words_freq, key = lambda x: x[1],
                reverse=True)
    return words_freq[:n]

top2_words = get_top_n2_words(corpus, n=-40)
top2_df = pd.DataFrame(top2_words)
top2_df.columns=["Bi-gram", "Freq"]
print(top2_df)

#Barplot of most freq Bi-grams
sns.set(rc={'figure.figsize':(13,8)})
h=sns.barplot(x="Bi-gram", y="Freq", data=top2_df)
h.set_xticklabels(h.get_xticklabels(), rotation=45)
#
#
#
#
#
#
# #Most frequently occuring Tri-grams
# def get_top_n3_words(corpus, n=None):
#     vec1 = CountVectorizer(ngram_range=(3,3),
#            max_features=2000).fit(corpus)
#     bag_of_words = vec1.transform(corpus)
#     sum_words = bag_of_words.sum(axis=0)
#     words_freq = [(word, sum_words[0, idx]) for word, idx in
#                   vec1.vocabulary_.items()]
#     words_freq =sorted(words_freq, key = lambda x: x[1],
#                 reverse=True)
#     return words_freq[:n]
# top3_words = get_top_n3_words(corpus, n=20)
# top3_df = pd.DataFrame(top3_words)
# top3_df.columns=["Tri-gram", "Freq"]
# print(top3_df)
# #Barplot of most freq Tri-grams
# import seaborn as sns
# sns.set(rc={'figure.figsize':(13,8)})
# j=sns.barplot(x="Tri-gram", y="Freq", data=top3_df)
# j.set_xticklabels(j.get_xticklabels(), rotation=45)



# #Most frequently occuring Quad-grams?
# def get_top_n4_words(corpus, n=None):
#     vec1 = CountVectorizer(ngram_range=(4,4),
#            max_features=2000).fit(corpus)
#     bag_of_words = vec1.transform(corpus)
#     sum_words = bag_of_words.sum(axis=0)
#     words_freq = [(word, sum_words[0, idx]) for word, idx in
#                   vec1.vocabulary_.items()]
#     words_freq =sorted(words_freq, key = lambda x: x[1],
#                 reverse=True)
#     return words_freq[:n]



# top4_words = get_top_n4_words(corpus, n=20)
# top4_df = pd.DataFrame(top4_words)
# top4_df.columns=["Quad-gram", "Freq"]
# print(top4_df)


#Barplot of most freq Tri-grams
import seaborn as sns
sns.set(rc={'figure.figsize':(13,8)})
j=sns.barplot(x="Quad-gram", y="Freq", data=top4_df)
j.set_xticklabels(j.get_xticklabels(), rotation=45)

###########################################################


# Define function to analyse text and pull out keywords within a given column of choice

def keyword_extractor(df, column):

    #############################
    # Data manipulation
    #############################

    # Define a list of all words to be removed from further analysis
    cull_words = [
        'and', 'and', 'the', 'this', 'as', 'be', 'a', 'by', 'with', 'been', 'very', 'are', 'contour',
        'from', '#NAME?', 'both', 'up', 'an', 'gases', '(â‰¥', 'at', 'E', 'while', 'although', 'annd',
        'Agree', 'ansd', 'these', 'along', 'The', 'adjacent', 'baed', 'contour)', 'of',
        'to', 'in', 'support', 'is', 'part', 'assigned', 'general', 'thin', 'that',
        'similar', 'levels', 'more', 'oil', 'outside', 'may', 'categorised', 'supplied',
        'scour', 'project', 'level', 'so', 'due', 'but', 'numbers', 'table', 'requirement', 'large',
        "gases'", 'Out', 'majority', 'north', 'note', 'soft', 'made', 'shows', 'layer', 'It', 'three',
        'northern', 'same', 'who', 'most', 'thorughout', 'map', 'it', 'centre', 'agree', 'jncc', 'name'
    ]

    # Develop a set type object to store all stopwords to be removed from the analysis
    stop_words = set()
    # Add custom stopwords to the stopwords list
    stop_words = stop_words.union(cull_words)

    # Develop text corpus and clean text data of unwanted elements
    corpus = []
    for i in range(0, 294):
        # Remove punctuations
        text = re.sub('[^a-zA-Z]', ' ', df[column].astype(str)[i])

        # Convert to lowercase
        text = text.lower()
        # remove tags
        text = re.sub("&lt;/?.*?&gt;", " &lt;&gt; ", text)
        # remove special characters and digits
        text = re.sub("(\\d|\\W)+", " ", text)
        # Convert to list from string
        text = text.split()
        # Stemming
        ps = PorterStemmer()
        # Lemmatisation
        lem = WordNetLemmatizer()
        text = [lem.lemmatize(word) for word in text if word not in stop_words]
        text = " ".join(text)
        corpus.append(text)

    # The two key parts of this process include Tokenisation and Vectorisation.
    # To complete this process of text preparation, we utilise the bag of words model, a technique which ignores the
    # sequence of words, and only accounts or the frequencies of occurrence

    # Utilise the sklearn CountVectorizer to tokenise the text and develop a vocabulary of known words.
    cv = CountVectorizer(
        max_df=1,
        # Ignore terms with a document frequency above this threshold (corpus specific words) - not sure if we
        # want this?
        stop_words=stop_words,
        max_features=10000,  # Maximum columns within the matrix
        ngram_range=(1, 3))  # Determines the list of words - single, bi-gram and tri-gram word combinations

    # Utilise the fit_transform function to learn and develop the library
    X = cv.fit_transform(corpus)

    #############################
    # Data visualisation
    #############################

    # Develop a data visualisation to represent the most commonly used 4 word sequences

    vec1 = CountVectorizer(ngram_range=(4, 4),
                           max_features=2000).fit(corpus)
    bag_of_words = vec1.transform(corpus)
    sum_words = bag_of_words.sum(axis=0)
    words_freq = [(word, sum_words[0, idx]) for word, idx in
                  vec1.vocabulary_.items()]
    words_freq = sorted(words_freq, key=lambda x: x[1],
                        reverse=True)

    # Define a DF object containing the top 20 four word combinations
    top_20 = words_freq[:20]
    top_20_df = pd.DataFrame(top_20)
    top_20_df.columns = ["Quad-gram", "Freq"]
    return top_20_df



keyword_extractor(Biotopes_DB, '1. Northern North Sea ')

