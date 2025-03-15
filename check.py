from xquery_converter import XQueryToMongoCRUDConverter

converter = XQueryToMongoCRUDConverter()
xquery = 'insert node <user><name>John</name><age>30</age></user> into collection("users")'
result = converter.parse_xquery(xquery)
print(result)
