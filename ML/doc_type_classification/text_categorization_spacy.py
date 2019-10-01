import random
import re
import math

from pathlib import Path

import spacy
from spacy.util import minibatch, compounding

from ml_service.spacy_utils import select_nlp, format_label_spacy
from ml_service.ml_utils import get_key_max_value
from ml_service.spacy_utils import normalize, clean_punctuation

CONTENTS = [
    "James lived nearby Samuel",
    "Maitre Samuel Halff is in da house",
    "Maitre Samuel Halff is in da house",
    "Sam is our CEO",
    "Nico is our CTO from Delémont",
    " Delémont est le chef lieu du Jura, ou pas.",
    "Nico is our CTO from Delémont",
    " Delémont est le chef lieu du Jura, ou pas.",
]

LABELS = [
    {"SH": True, "NC": False},
    {"SH": True, "NC": False},
    {"SH": False, "NC": True},
    {"SH": False, "NC": True},
]
LABEL_VALUES = ["SH", "SH", "SH", "SH", "NC", "NC", "NC", "NC"]

# TODO: Once validated, give only label to use for classification and add function to get the information from the DB + train/test split
# TODO: Deal with multiple languages? Multiple models: filter training docs by language label, then train model and store with lang extension
# TODO: Storage? Decide where to store and how
# TODO: Split training code into : _utils
# TODO: Current code is not memory efficient (train_data cast as a list)

def train_document_type_classifier(
    contents=CONTENTS,
    categories=LABEL_VALUES,
    language=None,
    output_dir="test_model.spacy",
    n_iter=5,
    train_fraction=0.5,
    init_tok2vec=None,
):
    '''
    if output_dir is not None:
        output_dir = Path(output_dir)
        if not output_dir.exists():
            output_dir.mkdir()
    '''
    if language is not None:
        # load pretrained modesl for language or blank?
        # nlp = spacy.load(model)  # load existing spaCy model
        nlp = spacy.blank(language)  # create blank Language class
        print("Created blank '%s' model" % language)
    else:
        nlp = spacy.blank("en")  # create blank Language class
        print("Created blank 'en' model")

    # add the text classifier to the pipeline if it doesn't exist
    # nlp.create_pipe works for built-ins that are registered with spaCy
    if "textcat" not in nlp.pipe_names:
        textcat = nlp.create_pipe(
            "textcat", config={"exclusive_classes": True, "architecture": "simple_cnn"}
        )
        # config={"exclusive_classes": True, "architecture": "bow"}
        nlp.add_pipe(textcat, last=True)
    # otherwise, get it, so we can add labels to it
    else:
        textcat = nlp.get_pipe("textcat")

    print(f"Length contents: {len(contents)}")
    print(f"Length categories: {len(categories)}")

    # add label to text classifier
    print(f"LIST CATEGORIES: {list(set(categories))}")
    [textcat.add_label(case_label) for case_label in list(set(categories))]

    # Prepare data (TODO:switch to generators for contents, remove dev)

    categories = list(format_label_spacy(categories))

    data_indices = list(range(len(contents)))
    random.shuffle(data_indices)
    contents = [normalize(clean_punctuation(contents[i]), lowercase=True, remove_stopwords=True) for i in data_indices]
    categories = [categories[i] for i in data_indices]

    train_cutoff = math.floor(train_fraction * len(contents))

    train_texts = contents[:train_cutoff]
    train_cats = categories[:train_cutoff]
    train_data = list(zip(train_texts, [{"cats": cats} for cats in train_cats]))

    dev_texts = contents[train_cutoff:]
    dev_cats = categories[train_cutoff:]
    dev_data = list(zip(dev_texts, [{"cats": cats} for cats in dev_cats]))

    # TODO: To remove test here which is a sanity check - should be moved to unittest
    test_text = dev_texts  # [0]  # [:2]

    print(
        "Using {} examples ({} training, {} evaluation)".format(
            len(contents), len(train_data), len(dev_data)
        )
    )

    # get names of other pipes to disable them during training
    other_pipes = [pipe for pipe in nlp.pipe_names if pipe != "textcat"]
    with nlp.disable_pipes(*other_pipes):  # only train textcat
        optimizer = nlp.begin_training()
        if init_tok2vec is not None:
            with init_tok2vec.open("rb") as file_:
                textcat.model.tok2vec.from_bytes(file_.read())
        print("Training the model...")
        print("{:^5}\t{:^5}\t{:^5}\t{:^5}".format("LOSS", "P", "R", "F"))
        batch_sizes = compounding(4.0, 32.0, 1.001)
        for i in range(n_iter):
            losses = {}
            # batch up the examples using spaCy's minibatch
            random.shuffle(train_data)
            batches = minibatch(train_data, size=batch_sizes)
            for batch in batches:
                texts, annotations = zip(*batch)
                nlp.update(texts, annotations, sgd=optimizer, drop=0.2, losses=losses)
            with textcat.model.use_params(optimizer.averages):
                # evaluate on the dev data split off in load_data()
                scores = evaluate(nlp.tokenizer, textcat, dev_texts, dev_cats)
            print(
                "{0:.3f}\t{1:.3f}\t{2:.3f}\t{3:.3f}".format(  # print a simple table
                    losses["textcat"], scores["textcat_p"], scores["textcat_r"], scores["textcat_f"]
                )
            )

    if output_dir is not None:
        with nlp.use_params(optimizer.averages):
            nlp.to_disk(output_dir)
        print("Saved model to", output_dir)

        # test the saved model
        print("Loading from", output_dir)
        nlp = spacy.load(output_dir)

    # test the trained model
    for test in test_text:
        doc = nlp(test)
        print(test[:10], doc.cats)

    return [scores["textcat_p"], scores["textcat_r"], scores["textcat_f"]]


def infer_content_classifier(contents, model_dir):
    print("Loading from", model_dir)
    nlp = spacy.load(model_dir)
    # TODO: Cleanup/pre-process text? Do also for training...
    results = []
    for text in contents:
        text = normalize(clean_punctuation(text), lowercase=True, remove_stopwords=True)
        doc = nlp(text)
        lab, confidence = get_key_max_value(doc.cats)
        results.append({"label_value": lab, "confidence": confidence})
    return results


def evaluate(tokenizer, textcat, texts, cats):
    docs = (tokenizer(text) for text in texts)
    tp = 0.0  # True positives
    fp = 1e-8  # False positives
    fn = 1e-8  # False negatives
    tn = 0.0  # True negatives
    for i, doc in enumerate(textcat.pipe(docs)):
        gold = cats[i]
        for label, score in doc.cats.items():
            if label not in gold:
                continue
            if score >= 0.5 and gold[label] >= 0.5:
                tp += 1.0
            elif score >= 0.5 and gold[label] < 0.5:
                fp += 1.0
            elif score < 0.5 and gold[label] < 0.5:
                tn += 1
            elif score < 0.5 and gold[label] >= 0.5:
                fn += 1
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)
    if (precision + recall) == 0:
        f_score = 0.0
    else:
        f_score = 2 * (precision * recall) / (precision + recall)
    return {"textcat_p": precision, "textcat_r": recall, "textcat_f": f_score}
