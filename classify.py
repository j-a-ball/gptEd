import json
import os
import fasttext
import multiprocessing
from tqdm import tqdm
from s2_fos import S2FOS
from collections import defaultdict
import warnings
import math

warnings.filterwarnings("ignore")
fasttext.FastText.eprint = lambda *args, **kwargs: None

def process_json_file(file_path, fasttext_model_path, num_workers):
    with open(file_path, "r") as file:
        data = json.load(file)

    articles = [(setspec, article) for setspec, articles in data.items() for article in articles]
    chunk_size = math.ceil(len(articles) / num_workers)
    article_chunks = [articles[i:i + chunk_size] for i in range(0, len(articles), chunk_size)]

    with multiprocessing.Pool(num_workers, initializer=init_worker, initargs=(fasttext_model_path,)) as pool:
        results = list(tqdm(pool.imap(process_chunk, article_chunks), total=len(article_chunks), desc=f"Processing {file_path}"))

    updated_data = defaultdict(list)
    for chunk_results in results:
        for setspec, article in chunk_results:
            updated_data[setspec].append(article)

    save_updated_json(file_path, dict(updated_data))
    print_distributions(file_path, updated_data)

def init_worker(fasttext_model_path):
    global fasttext_model
    global fos_predictor
    fasttext_model = fasttext.load_model(fasttext_model_path)
    fos_predictor = S2FOS()

def process_chunk(chunk):
    results = []
    for setspec, article in chunk:
        text = " ".join(article.get("title", []) + article.get("description", []) + article.get("publisher", []))
        predicted_language = predict_language(text)
        article["predicted_language"] = [(label.replace("__label__", ""), score) for label, score in zip(predicted_language[0], predicted_language[1])]
        if article["predicted_language"][0][0] == "en":
            predicted_fos = predict_fos(article)
            article["predicted_fos"] = list(tuple(score) for score in predicted_fos["scores"][0])
        else:
            article["predicted_fos"] = []
        results.append((setspec, article))
    return results

def predict_language(text):
    return fasttext_model.predict(text.replace("\n", " "))

def predict_fos(article):
    fos_input = [{
        "title": " ".join(article.get("title", [])),
        "abstract": " ".join(article.get("description", [])),
        "journal_name": " ".join(article.get("publisher", []))
    }]
    return fos_predictor.predict(fos_input)

def save_updated_json(file_path, data):
    with open(file_path, "w") as file:
        json.dump(data, file, indent=2)

def print_distributions(file_path, data):
    year = os.path.splitext(os.path.basename(file_path))[0]
    language_counts = defaultdict(int)
    discipline_counts = defaultdict(int)
    total_articles = sum(len(articles) for articles in data.values())

    for articles in data.values():
        for article in articles:
            if article.get("predicted_language", []):
                language_counts[article["predicted_language"][0][0]] += 1
            if article.get("predicted_fos", []):
                discipline_counts[article["predicted_fos"][0][0]] += 1

    print(f"Year: {year}")
    print("Language Distribution:")
    for language, count in sorted(language_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = count / total_articles * 100
        print(f"{language}: {count} ({percentage:.2f}%)")

    print("\nDiscipline Distribution:")
    for discipline, count in sorted(discipline_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = count / total_articles * 100
        print(f"{discipline}: {count} ({percentage:.2f}%)")

    print("\n")

if __name__ == "__main__":
    fasttext_model_path = "s2_fos/data/lid.176.bin"
    json_dir = "data/json/"
    num_workers = 15
    # already completed 2024
    for file in ["2023.json", "2022.json", "2021.json"]:
        process_json_file(os.path.join(json_dir, file), fasttext_model_path, num_workers)