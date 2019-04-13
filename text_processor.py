from nltk.stem.porter import PorterStemmer
from pymongo import MongoClient
from tqdm import tqdm
import math

client = MongoClient()
db = client['riw_db']


class Utls:
    def __init__(self, db):
        self.stop_words = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"]
        self.exception = ["NUMEPROPRII"]
        self.separators = ['.', ' ', ',', ';', ':', '!', '?', '*', '\n', '\t', '"', '\\', '$']
        self.db = db

    def PorterStemmer(self, word):
        porter_stemmer = PorterStemmer()
        return porter_stemmer.stem(word)

    def QueryResolver(self, query):
        if query.strip() == "" or query is None:
            return

        invers_index_coll = self.db["invers_index"]
        direct_index_coll = self.db["direct_index"]
        docs_vectors_coll = self.db["docs_vectors"]
        query = query.strip().split(' ')
        porter_stemmer = PorterStemmer()

        vector = {}
        squres_sum = 0
        for word in query:
            word = porter_stemmer.stem(word)

            if word not in self.exception and word not in self.stop_words or word in self.stop_words and word in self.exception:
                docs_list = invers_index_coll.find_one({"term": word})["docs"]
            else:
                continue

            if len(docs_list) < 1:
                continue

            tf = 1 / len(query)
            idf = math.log((direct_index_coll.count() + 1) / (1 + len(docs_list)), 2)
            coefficient = tf * idf
            squres_sum += coefficient * coefficient
            vector[word] = coefficient
        module = math.sqrt(squres_sum)

        # query vector and module

        cosinus_similar = []

        for doc in docs_vectors_coll.find():
            vector_produs = 0
            for word in vector:
                if word in doc["vector"]:
                    vector_produs += vector[word] * doc["vector"][word]

            cos = vector_produs / (doc["module"] * module)
            cosinus_similar.append({"file": doc["file"], "cos": cos})
        sorted_cosinus = sorted(cosinus_similar, key=lambda v: v["cos"], reverse=True)
        files_in_order = [i["file"] for i in sorted_cosinus]
        return files_in_order


    def Create_docs_vectors(self):
        db = self.db
        direct_index_coll = db["direct_index"]
        invers_index_coll = db["invers_index"]
        docs_vectors_coll = db["docs_vectors"]


        for doc in tqdm(direct_index_coll.find()):
            # doc["file"]
            num_of_docs = direct_index_coll.count()
            total_num_words_in_doc = 0
            for item in doc["terms"]:
                total_num_words_in_doc += item["freq"]

            vector = {}
            squares_sum = 0
            for item in doc["terms"]:
                # get the list of documents where the term  < item["term"] > is found
                docs_list = invers_index_coll.find_one({"term": item["term"]})["docs"]
                # if the word can not be found in any documents, but it should not get at that point
                if not len(docs_list):
                    continue

                idf = math.log(num_of_docs / len(docs_list), 2)
                tf = item["freq"] / total_num_words_in_doc
                coefficient = tf * idf
                if coefficient > 0:
                    vector[item["term"]] = coefficient
                    squares_sum += coefficient * coefficient

            docs_vectors_coll.insert_one({"file": doc["file"], "module": math.sqrt(squares_sum), "vector": vector})


Utils = Utls(db)


if __name__ == "__main__":
    Utils.Create_docs_vectors()
