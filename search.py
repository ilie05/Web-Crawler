from text_processor import Utils

if __name__ == '__main__':

    while True:
        query = input("Search query: ")
        if query == 'Exit()':
            break
        docs = Utils.QueryResolver(query)
        for doc in docs:
            print(doc)