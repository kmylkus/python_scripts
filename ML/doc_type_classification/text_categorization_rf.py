import re
import pickle
import os

import numpy as np
from flask import current_app as app
import spacy
import requests
import logging

CLASSIFY_MODELS = "/models/classify/"


def classify_content(content):

    app.logger.debug("Start classification")

    nlp = spacy.load("en", disable=["parser", "tagger", "ner"])

    def entity_strip(text):
        for sentence in text.splitlines():
            stripped_sentence = nlp(sentence)
            lemmas = (
                token.lemma_.strip()
                for token in stripped_sentence
                if not token.is_stop and token.lemma_.strip()
            )

            yield " ".join(lemmas)

    cleaned = re.sub(r"[\t ]+", " ", content).lower()

    clean_doc = [" ".join(entity_strip(cleaned))]

    with open(os.path.join(CLASSIFY_MODELS, "vectorizer"), "rb") as f:
        u = pickle._Unpickler(f)
        u.encoding = "utf-8"
        vectorizer = u.load()

    try:
        X = vectorizer.transform(clean_doc).toarray()
    except ValueError as error:
        app.logger.error("Error while applying vectorizer to document content: \n{} ".format(error))

    with open(os.path.join(CLASSIFY_MODELS, "text_classifier"), "rb") as f:
        u = pickle._Unpickler(f)
        u.encoding = "utf-8"
        model = u.load()

    try:
        result = model.predict(X)
        app.logger.debug("result: {}".format(result[0]))
    except ValueError as error:
        app.logger.error("Error while predicting document type: \n{}".format(error))

    try:
        result_proba = model.predict_proba(X)
        result_proba = np.around(np.amax(result_proba) * 100, decimals=1)
        app.logger.debug("probability: {}".format(result_proba))
    except ValueError as error:
        app.logger.error("Error while evaluating certainty of prediction: \n{}".format(error))

    app.logger.info("Classification process over")

    return result[0], result_proba
