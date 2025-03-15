import pymongo
import re

def connect_mongo(uri="mongodb://localhost:27017/", db_name="testDB", collection_name="testCollection"):
    """Connects to MongoDB and returns a collection handle."""
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    return db[collection_name]

def convert_xquery_to_mongo(xquery):
    """Converts MarkLogic XQuery CRUD operations to MongoDB equivalents."""
    if "xdmp:document-insert" in xquery:
        # Create: Insert Document
        match = re.search(r'xdmp:document-insert\("(.+?)", (.+)\)', xquery)
        if match:
            doc_id = match.group(1)
            document = eval(match.group(2))  # Converts XML-like string to dict (simplified parsing)
            document["_id"] = doc_id  # Store the document ID
            return {"operation": "insert", "data": document}
    
    elif "xdmp:document-get" in xquery:
        # Read: Find Document
        match = re.search(r'xdmp:document-get\("(.+?)"\)', xquery)
        if match:
            doc_id = match.group(1)
            return {"operation": "find", "query": {"_id": doc_id}}
    
    elif "xdmp:node-replace" in xquery:
        # Update: Replace Field
        match = re.search(r'xdmp:node-replace\(doc\("(.+?)"\)//(.+?), (.+)\)', xquery)
        if match:
            doc_id = match.group(1)
            field = match.group(2)
            new_value = eval(match.group(3))
            return {"operation": "update", "query": {"_id": doc_id}, "update": {"$set": {field: new_value}}}
    
    elif "xdmp:document-delete" in xquery:
        # Delete: Remove Document
        match = re.search(r'xdmp:document-delete\("(.+?)"\)', xquery)
        if match:
            doc_id = match.group(1)
            return {"operation": "delete", "query": {"_id": doc_id}}
    
    return {"error": "Unsupported XQuery statement"}

def execute_mongo_query(mongo_collection, mongo_query):
    """Executes the MongoDB equivalent operation."""
    operation = mongo_query.get("operation")
    
    if operation == "insert":
        result = mongo_collection.insert_one(mongo_query["data"])
        return f"Inserted Document with ID: {result.inserted_id}"
    
    elif operation == "find":
        document = mongo_collection.find_one(mongo_query["query"])
        return document if document else "Document not found"
    
    elif operation == "update":
        result = mongo_collection.update_one(mongo_query["query"], mongo_query["update"])
        return f"Updated {result.modified_count} document(s)"
    
    elif operation == "delete":
        result = mongo_collection.delete_one(mongo_query["query"])
        return f"Deleted {result.deleted_count} document(s)"
    
    return "Invalid operation"

if __name__ == "__main__":
    # Example XQuery statement (Change this to test different CRUD operations)
    xquery_statement = 'xdmp:document-insert("/books/newbook.xml", {"title": "New Book"})'
    
    # Convert XQuery to MongoDB command
    mongo_query = convert_xquery_to_mongo(xquery_statement)
    
    # Connect to MongoDB
    mongo_collection = connect_mongo()
    
    # Execute MongoDB operation
    result = execute_mongo_query(mongo_collection, mongo_query)
    
    print("MongoDB Query Result:", result)
