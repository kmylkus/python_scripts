
def extract_tokens(files, reset=False, case=True):
    valid_texts = get_clean_text_with_meta(files, 1000000)

    contents, metas = unzip_2(valid_texts)

    docs = nlp_min.pipe((content.lower() for content in contents), batch_size=10, n_threads=-1)
    metas = (meta for meta in metas)

    if reset:
        reset_counts()
    
    for doc, meta in zip(docs, metas):
        logging.info("Extracting words of file : " + meta["file_name"])

        if case:
            case_name = meta["labels"][0]["label"]
            case_collection.update_one({"case": case_name}, {"$addToSet": {"files": meta["_id"]}}, upsert=True)
            case_id = case_collection.find_one({"case": case_name}, {"_id": 1})["_id"]
        
        file_collection.update_one({"_id": meta["_id"]}, {"$set": {"tokenized": True}})

        tokens = {}
        doc_count = 0
        for token in doc:
            if not token.is_stop and token.lemma_.strip():
                token_text = token.lemma_.strip()
                if re.fullmatch(r"[a-zA-Z]+", token_text):
                    current_count = tokens.get(token_text, 0)
                    tokens[token_text] = current_count + 1
                    doc_count += 1
        
        logging.info("adding {} tokens to file : {}".format( len(tokens.values()), meta["file_name"]))

        if case:
            add_tokens_mongo(tokens, doc_count, meta["_id"], case_id=case_id)
        else:
            add_tokens_mongo(tokens, doc_count, meta["_id"])