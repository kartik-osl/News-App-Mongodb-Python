# Import necessary libraries
import argparse
import pandas as pd
import spacy
import csv
from textblob import TextBlob
from pymongo import MongoClient, UpdateMany
from datetime import datetime



#----------------------------------------------------------------------------------------------------





# Initialize spaCy and MongoDB
naturalLangProcesing = spacy.load("en_core_web_sm")
client = MongoClient("mongodb+srv://agni5kartik:K12Entre@cluster0.caiqbmp.mongodb.net/") 
db = client["news_database"]
# headlines_collection = db["headlines"]
headlines_collection = db["small2"]






#----------------------------------------------------------------------------------------------------




# Without Panda
def import_headlines(csv_path):
    start_time = datetime.now()  # Record start time

    # Open the CSV file for reading
    with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
        csvreader = csv.DictReader(csvfile)

        batch_size = 5000
        batch = []

        for row in csvreader:
            # Convert each row to a dictionary
            headline = {
                "headline_text": row["headline_text"],
                # Add other fields as needed
            }

            # Add the dictionary to the batch
            batch.append(headline)

            # If the batch size is reached, insert it into MongoDB and clear the batch
            if len(batch) >= batch_size:
                headlines_collection.insert_many(batch)
                batch = []

        # Insert any remaining records in the last batch
        if batch:
            headlines_collection.insert_many(batch)

    print("Headlines imported successfully.")
    print(f"Execution time: {datetime.now() - start_time}")

# With Panda    
# def import_headlines(csv_path, batch_size=5000):
#     start_time = datetime.now()  # Record start time
#     # Read the CSV file in chunks
#     chunks = pd.read_csv(csv_path, chunksize=batch_size)

#     for chunk in chunks:
#         headlines = chunk.to_dict(orient="records")
#         # print(headlines)
#         headlines_collection.insert_many(headlines)

#     print("Headlines imported successfully.")
        
#     # Calculate and print execution time





#----------------------------------------------------------------------------------------------------





def extract_entities():
    # Process headlines and store results in MongoDB
    start_time = datetime.now()  # Record start time
    for headline in headlines_collection.find():
        doc = naturalLangProcesing(headline["headline_text"])
        
        # Filter entities to include only "per," "org," and "loc" types
        entities = [{"text": ent.text, "type": ent.label_} for ent in doc.ents if ent.label_ in {"PER", "ORG", "LOC"}]
        
        sentiment = TextBlob(headline["headline_text"]).sentiment.polarity

        # Define the update operation condition
        update_condition = {"_id": headline["_id"]}
        
        # Define the update operation
        update_operation = UpdateMany(
            update_condition,
            {
                "$set": {
                    "identified_entities": entities,
                    "sentiment": "positive" if sentiment > 0 else "negative" if sentiment < 0 else "neutral",
                }
            },
        )

        # Execute the update operation
        headlines_collection.bulk_write([update_operation])
    print(f"Execution time: {datetime.now() - start_time}")
    print(db.headlines_collection.count())




#----------------------------------------------------------------------------------------------------





def top_100_entities_with_type():
    start_time = datetime.now()  
    # Unwinding, grouping and aggregating the newly formed array
    the_array = [
        {"$unwind": "$identified_entities"},
        {"$group": {"_id": {"text": "$identified_entities.text", "type": "$identified_entities.type"}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 100},
    ]
    top_entities = list(headlines_collection.aggregate(the_array))
    for entity in top_entities:
        print(f"{entity['_id']['text']} ({entity['_id']['type']}): {entity['count']} mentions")
    
    print(f"Execution time: {datetime.now() - start_time}")





#----------------------------------------------------------------------------------------------------





def all_headlines_for(entity_name):
    start_time = datetime.now()  
    # Retrieve and print all headlines associated with the given entity name
    entity_name = entity_name.lower()
    for headline in headlines_collection.find({"identified_entities.text": entity_name}):
        print(f"{headline['_id']}: {headline['headline_text']}")
    
    print(f"Execution time: {datetime.now() - start_time}")





#----------------------------------------------------------------------------------------------------





if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="News Headline Processing and Analysis CLI")
    subparsers = parser.add_subparsers(title="subcommands")

    import_parser = subparsers.add_parser("import-headlines")
    import_parser.add_argument("csv_path", help="Path to the CSV file containing headlines")
    import_parser.set_defaults(func="import_headlines")

    extract_parser = subparsers.add_parser("extract-entities")
    extract_parser.set_defaults(func="extract_entities")

    top_100_parser = subparsers.add_parser("top100entitieswithtype")
    top_100_parser.set_defaults(func="top_100_entities_with_type")

    all_headlines_parser = subparsers.add_parser("allheadlinesfor")
    all_headlines_parser.add_argument("entity_name", help="Entity name to retrieve headlines for")
    all_headlines_parser.set_defaults(func="all_headlines_for")

    args = parser.parse_args()

    if args.func == "import_headlines":
        import_headlines(args.csv_path)
    elif args.func == "extract_entities":
        extract_entities()
    elif args.func == "top_100_entities_with_type":
        top_100_entities_with_type()
    elif args.func == "all_headlines_for":
        all_headlines_for(args.entity_name)
