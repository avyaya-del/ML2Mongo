import re

def convert_xquery_to_mongodb(xquery_command):
    # Define patterns for CRUD operations in XQuery
    patterns = {
        'create': re.compile(r'xdmp:document-insert\(\s*"([^"]+)"\s*,\s*(.+)\s*\)'),
        'read': re.compile(r'fn:doc\(\s*"([^"]+)"\s*\)'),
        'update': re.compile(r'xdmp:node-replace\(\s*"([^"]+)"\s*,\s*(.+)\s*\)'),
        'delete': re.compile(r'xdmp:document-delete\(\s*"([^"]+)"\s*\)')
    }
    # Define MongoDB equivalents
    mongodb_commands = {
        'create': lambda match: f'db.collection.insertOne({{ "_id": "{match.group(1)}", "data": {match.group(2)} }})',
        'read': lambda match: f'db.collection.findOne({{ "_id": "{match.group(1)}" }})',
        'update': lambda match: f'db.collection.updateOne({{ "_id": "{match.group(1)}" }}, {{ $set: {match.group(2)} }})',
        'delete': lambda match: f'db.collection.deleteOne({{ "_id": "{match.group(1)}" }})'
    }
    # Check for matches and convert
    for operation, pattern in patterns.items():
        match = pattern.search(xquery_command)
        if match:
            return mongodb_commands[operation](match)
    return "Unsupported XQuery command"

# Example usage
xquery_create = 'xdmp:document-insert("/example.json", {"name": "John", "age": 30})'
xquery_read = 'fn:doc("/example.json")'
xquery_update = 'xdmp:node-replace("/example.json", {"name": "John", "age": 31})'
xquery_delete = 'xdmp:document-delete("/example.json")'

print(convert_xquery_to_mongodb(xquery_create))  # Should output: db.collection.insertOne({ "_id": "/example.json", "data": {"name": "John", "age": 30} })
print(convert_xquery_to_mongodb(xquery_read))    # Should output: db.collection.findOne({ "_id": "/example.json" })
print(convert_xquery_to_mongodb(xquery_update))  # Should output: db.collection.updateOne({ "_id": "/example.json" }, { $set: {"name": "John", "age": 31} })
print(convert_xquery_to_mongodb(xquery_delete))  # Should output: db.collection.deleteOne({ "_id": "/example.json" })
