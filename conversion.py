import re
import json
import sys
import argparse
from lxml import etree

class XQueryToMongoCRUDConverter:
    def __init__(self):
        self.var_mappings = {}
        
    def parse_xquery(self, xquery_string):
        """Parse an XQuery CRUD statement and convert it to a MongoDB operation"""
        
        # Clean and normalize the input
        xquery_string = xquery_string.strip()
        
        # Handle insert/create operations
        if re.search(r'insert\s+node', xquery_string, re.IGNORECASE) or re.search(r'db\.collection\(["\'][^"\']+["\']\)\.insert', xquery_string, re.IGNORECASE):
            return self._parse_insert(xquery_string)
            
        # Handle update operations
        elif re.search(r'replace\s+node|update\s+value', xquery_string, re.IGNORECASE) or re.search(r'db\.collection\(["\'][^"\']+["\']\)\.update', xquery_string, re.IGNORECASE):
            return self._parse_update(xquery_string)
            
        # Handle delete operations
        elif re.search(r'delete\s+node', xquery_string, re.IGNORECASE) or re.search(r'db\.collection\(["\'][^"\']+["\']\)\.remove', xquery_string, re.IGNORECASE):
            return self._parse_delete(xquery_string)
        
        # Handle read/query operations
        elif re.search(r'for\s+\$\w+\s+in\s+collection|db\.collection\(["\'][^"\']+["\']\)\.find', xquery_string, re.IGNORECASE):
            return self._parse_read(xquery_string)
        
        else:
            raise ValueError(f"Unsupported XQuery CRUD syntax: {xquery_string}")
    
    def _parse_insert(self, xquery_string):
        """Parse an XQuery insert statement"""
        
        # MongoDB-like syntax: db.collection("collectionName").insert({...})
        mongo_style_match = re.search(r'db\.collection\(["\']([^"\']+)["\']\)\.insert\((.*)\)', xquery_string, re.IGNORECASE | re.DOTALL)
        if mongo_style_match:
            collection_name = mongo_style_match.group(1)
            doc_json = mongo_style_match.group(2).strip()
            
            # Try to parse the JSON document
            try:
                # Remove any trailing commas and clean up the JSON
                doc_json = re.sub(r',\s*}', '}', doc_json)
                doc_json = re.sub(r',\s*]', ']', doc_json)
                
                # Parse the document as JSON
                doc = json.loads(doc_json)
                
                return {
                    "collection": collection_name,
                    "operation": "insertOne",
                    "document": doc
                }
            except json.JSONDecodeError as e:
                raise ValueError(f"Could not parse insert document JSON: {e}")
        
        # XQuery style: insert node <element>...</element> into collection("collectionName")
        xquery_style_match = re.search(r'insert\s+node\s+(.*?)\s+into\s+collection\(["\']([^"\']+)["\']\)', xquery_string, re.IGNORECASE | re.DOTALL)
        if xquery_style_match:
            xml_content = xquery_style_match.group(1).strip()
            collection_name = xquery_style_match.group(2)
            
            # Try to parse the XML and convert to JSON
            try:
                # If it's a direct JSON object
                if xml_content.startswith('{') and xml_content.endswith('}'):
                    try:
                        doc = json.loads(xml_content)
                    except json.JSONDecodeError:
                        raise ValueError("Could not parse insert JSON content")
                # If it's XML
                else:
                    # Remove any XML declaration
                    xml_content = re.sub(r'<\?xml[^>]+\?>', '', xml_content)
                    
                    # Parse XML to JSON
                    try:
                        xml_tree = etree.fromstring(xml_content)
                        doc = self._xml_to_json(xml_tree)
                    except Exception as e:
                        raise ValueError(f"Could not parse XML: {e}")
                
                return {
                    "collection": collection_name,
                    "operation": "insertOne",
                    "document": doc
                }
            except Exception as e:
                raise ValueError(f"Could not process insert operation: {e}")
        
        raise ValueError(f"Unsupported insert syntax: {xquery_string}")
    
    def _xml_to_json(self, element):
        """Convert an XML element to a JSON object"""
        result = {}
        
        # Handle attributes
        for key, value in element.attrib.items():
            result[f"@{key}"] = value
        
        # Handle child elements
        for child in element:
            child_dict = self._xml_to_json(child)
            tag = child.tag
            
            if tag in result:
                # If this tag already exists, convert to a list or append
                if isinstance(result[tag], list):
                    result[tag].append(child_dict)
                else:
                    result[tag] = [result[tag], child_dict]
            else:
                result[tag] = child_dict
        
        # Handle text content
        if element.text and element.text.strip():
            if result:  # Has attributes or children
                result["#text"] = element.text.strip()
            else:  # Just text
                return element.text.strip()
        
        return result
    
    def _parse_update(self, xquery_string):
        """Parse an XQuery update statement"""
        
        # MongoDB-like syntax: db.collection("collectionName").update({query}, {update})
        mongo_style_match = re.search(r'db\.collection\(["\']([^"\']+)["\']\)\.update\((.*?),\s*(.*)\)', xquery_string, re.IGNORECASE | re.DOTALL)
        if mongo_style_match:
            collection_name = mongo_style_match.group(1)
            query_json = mongo_style_match.group(2).strip()
            update_json = mongo_style_match.group(3).strip()
            
            try:
                # Clean up and parse the JSON
                query_json = re.sub(r',\s*}', '}', query_json)
                update_json = re.sub(r',\s*}', '}', update_json)
                
                query = json.loads(query_json)
                
                # Check if the update uses MongoDB operators
                if update_json.strip().startswith('{') and re.search(r'"\$set"|"\$inc"|"\$push"', update_json):
                    update = json.loads(update_json)
                else:
                    # If no MongoDB operators specified, wrap in $set
                    update = {"$set": json.loads(update_json)}
                
                return {
                    "collection": collection_name,
                    "operation": "updateMany",
                    "filter": query,
                    "update": update
                }
            except json.JSONDecodeError as e:
                raise ValueError(f"Could not parse update JSON: {e}")
        
        # XQuery style: replace/update statements
        replace_node_match = re.search(r'replace\s+node\s+(.*?)\s+with\s+(.*?)(?:where|in\s+collection)', xquery_string, re.IGNORECASE | re.DOTALL)
        if replace_node_match:
            target_path = replace_node_match.group(1).strip()
            replacement = replace_node_match.group(2).strip()
            
            # Extract collection name
            collection_match = re.search(r'in\s+collection\(["\']([^"\']+)["\']\)', xquery_string)
            if not collection_match:
                raise ValueError("Collection name not found in replace statement")
            
            collection_name = collection_match.group(1)
            
            # Extract where condition if present
            where_match = re.search(r'where\s+(.*?)(?:in\s+collection|\Z)', xquery_string, re.IGNORECASE | re.DOTALL)
            where_clause = where_match.group(1).strip() if where_match else None
            
            # Parse the target path and build query
            path_parts = re.search(r'\$(\w+)(?:/([^/]+))?', target_path)
            if not path_parts:
                raise ValueError(f"Could not parse target path: {target_path}")
            
            var_name = path_parts.group(1)
            field_path = path_parts.group(2).replace('/', '.') if path_parts.group(2) else None
            
            # Build query from where clause
            query = {}
            if where_clause:
                query = self._parse_where_clause(where_clause, var_name)
            
            # Parse the replacement value
            update_value = None
            if replacement.startswith('{') and replacement.endswith('}'):
                try:
                    update_value = json.loads(replacement)
                except json.JSONDecodeError:
                    update_value = replacement
            else:
                update_value = replacement
            
            # Build the update operation
            update = {}
            if field_path:
                update = {"$set": {field_path: update_value}}
            else:
                update = {"$set": update_value}
            
            return {
                "collection": collection_name,
                "operation": "updateMany",
                "filter": query,
                "update": update
            }
        
        # XQuery update value syntax
        update_value_match = re.search(r'update\s+value\s+(.*?)\s+with\s+(.*?)(?:where|in\s+collection)', xquery_string, re.IGNORECASE | re.DOTALL)
        if update_value_match:
            target_path = update_value_match.group(1).strip()
            new_value = update_value_match.group(2).strip()
            
            # Extract collection name
            collection_match = re.search(r'in\s+collection\(["\']([^"\']+)["\']\)', xquery_string)
            if not collection_match:
                raise ValueError("Collection name not found in update statement")
            
            collection_name = collection_match.group(1)
            
            # Extract where condition if present
            where_match = re.search(r'where\s+(.*?)(?:in\s+collection|\Z)', xquery_string, re.IGNORECASE | re.DOTALL)
            where_clause = where_match.group(1).strip() if where_match else None
            
            # Parse the target path and build query
            path_parts = re.search(r'\$(\w+)(?:/([^/]+))?', target_path)
            if not path_parts:
                raise ValueError(f"Could not parse target path: {target_path}")
            
            var_name = path_parts.group(1)
            field_path = path_parts.group(2).replace('/', '.') if path_parts.group(2) else None
            
            # Build query from where clause
            query = {}
            if where_clause:
                query = self._parse_where_clause(where_clause, var_name)
            
            # Parse the new value
            if new_value.startswith('"') and new_value.endswith('"'):
                new_value = new_value[1:-1]  # Remove quotes
            elif new_value.isdigit():
                new_value = int(new_value)
            elif re.match(r'^[0-9]+\.[0-9]+$', new_value):
                new_value = float(new_value)
            
            # Build the update operation
            update = {"$set": {field_path: new_value}} if field_path else {"$set": new_value}
            
            return {
                "collection": collection_name,
                "operation": "updateMany",
                "filter": query,
                "update": update
            }
        
        raise ValueError(f"Unsupported update syntax: {xquery_string}")
    
    def _parse_delete(self, xquery_string):
        """Parse an XQuery delete statement"""
        
        # MongoDB-like syntax: db.collection("collectionName").remove({query})
        mongo_style_match = re.search(r'db\.collection\(["\']([^"\']+)["\']\)\.remove\((.*)\)', xquery_string, re.IGNORECASE | re.DOTALL)
        if mongo_style_match:
            collection_name = mongo_style_match.group(1)
            query_json = mongo_style_match.group(2).strip()
            
            try:
                # Clean up and parse the JSON
                query_json = re.sub(r',\s*}', '}', query_json)
                query = json.loads(query_json)
                
                return {
                    "collection": collection_name,
                    "operation": "deleteMany",
                    "filter": query
                }
            except json.JSONDecodeError as e:
                raise ValueError(f"Could not parse delete query JSON: {e}")
        
        # XQuery style: delete node statements
        delete_node_match = re.search(r'delete\s+node\s+(.*?)(?:where|in\s+collection)', xquery_string, re.IGNORECASE | re.DOTALL)
        if delete_node_match:
            target_path = delete_node_match.group(1).strip()
            
            # Extract collection name
            collection_match = re.search(r'in\s+collection\(["\']([^"\']+)["\']\)', xquery_string)
            if not collection_match:
                raise ValueError("Collection name not found in delete statement")
            
            collection_name = collection_match.group(1)
            
            # Extract where condition if present
            where_match = re.search(r'where\s+(.*?)(?:in\s+collection|\Z)', xquery_string, re.IGNORECASE | re.DOTALL)
            where_clause = where_match.group(1).strip() if where_match else None
            
            # Parse the target path and build query
            path_parts = re.search(r'\$(\w+)(?:/([^/]+))?', target_path)
            if not path_parts:
                raise ValueError(f"Could not parse target path: {target_path}")
            
            var_name = path_parts.group(1)
            
            # Build query from where clause
            query = {}
            if where_clause:
                query = self._parse_where_clause(where_clause, var_name)
            
            return {
                "collection": collection_name,
                "operation": "deleteMany",
                "filter": query
            }
        
        raise ValueError(f"Unsupported delete syntax: {xquery_string}")
    
    def _parse_read(self, xquery_string):
        """Parse an XQuery read/query statement"""
        
        # MongoDB-like syntax: db.collection("collectionName").find({query})
        mongo_style_match = re.search(r'db\.collection\(["\']([^"\']+)["\']\)\.find\((.*?)\)', xquery_string, re.IGNORECASE | re.DOTALL)
        if mongo_style_match:
            collection_name = mongo_style_match.group(1)
            query_json = mongo_style_match.group(2).strip()
            
            try:
                # Clean up and parse the JSON
                query_json =






query_json = re.sub(r',\s*}', '}', query_json)
                if not query_json:
                    query = {}
                else:
                    query = json.loads(query_json)
                
                return {
                    "collection": collection_name,
                    "operation": "find",
                    "filter": query
                }
            except json.JSONDecodeError as e:
                raise ValueError(f"Could not parse query JSON: {e}")
        
        # XQuery style: for $var in collection("collectionName") where ...
        xquery_style_match = re.search(r'for\s+\$(\w+)\s+in\s+collection\(["\']([^"\']+)["\']\)(.*)', xquery_string, re.IGNORECASE | re.DOTALL)
        if xquery_style_match:
            var_name = xquery_style_match.group(1)
            collection_name = xquery_style_match.group(2)
            rest_of_query = xquery_style_match.group(3).strip()
            
            # Save variable mapping
            self.var_mappings[var_name] = collection_name
            
            # Extract where condition if present
            where_match = re.search(r'where\s+(.*?)(?:order\s+by|\Z)', rest_of_query, re.IGNORECASE | re.DOTALL)
            where_clause = where_match.group(1).strip() if where_match else None
            
            # Build query from where clause
            query = {}
            if where_clause:
                query = self._parse_where_clause(where_clause, var_name)
            
            # Check for order by clause
            order_by_match = re.search(r'order\s+by\s+(.*?)(?:return|\Z)', rest_of_query, re.IGNORECASE | re.DOTALL)
            sort = None
            if order_by_match:
                sort_clause = order_by_match.group(1).strip()
                sort = self._parse_order_by(sort_clause, var_name)
            
            # Check for projection in return clause
            projection = None
            return_match = re.search(r'return\s+(.*)', rest_of_query, re.IGNORECASE | re.DOTALL)
            if return_match:
                return_clause = return_match.group(1).strip()
                projection = self._parse_return_clause(return_clause, var_name)
            
            result = {
                "collection": collection_name,
                "operation": "find",
                "filter": query
            }
            
            if sort:
                result["sort"] = sort
            
            if projection:
                result["projection"] = projection
            
            return result
        
        raise ValueError(f"Unsupported read syntax: {xquery_string}")
    
    def _parse_single_condition(self, condition, context_var):
        """Parse a single condition in a where clause into a MongoDB query operator"""
        
        # Strip any parentheses
        condition = condition.strip('()')
        
        # Check for equality condition: $item/field = "value" or $item/field eq "value"
        eq_match = re.search(r'(\$\w+(?:/[^/\s]+)*)\s*=\s*(.+)', condition) or re.search(r'(\$\w+(?:/[^/\s]+)*)\s+eq\s+(.+)', condition)
        if eq_match:
            path = eq_match.group(1).strip()
            value = eq_match.group(2).strip()
            field = self._parse_path(path, context_var)
            parsed_value = self._parse_value(value)
            
            return {field: parsed_value}
        
        # Check for inequality condition: $item/field != "value" or $item/field ne "value"
        ne_match = re.search(r'(\$\w+(?:/[^/\s]+)*)\s*!=\s*(.+)', condition) or re.search(r'(\$\w+(?:/[^/\s]+)*)\s+ne\s+(.+)', condition)
        if ne_match:
            path = ne_match.group(1).strip()
            value = ne_match.group(2).strip()
            field = self._parse_path(path, context_var)
            parsed_value = self._parse_value(value)
            
            return {field: {"$ne": parsed_value}}
        
        # Check for greater than condition: $item/field > value or $item/field gt value
        gt_match = re.search(r'(\$\w+(?:/[^/\s]+)*)\s*>\s*(.+)', condition) or re.search(r'(\$\w+(?:/[^/\s]+)*)\s+gt\s+(.+)', condition)
        if gt_match:
            path = gt_match.group(1).strip()
            value = gt_match.group(2).strip()
            field = self._parse_path(path, context_var)
            parsed_value = self._parse_value(value)
            
            return {field: {"$gt": parsed_value}}
        
        # Check for greater than or equal condition: $item/field >= value or $item/field ge value
        gte_match = re.search(r'(\$\w+(?:/[^/\s]+)*)\s*>=\s*(.+)', condition) or re.search(r'(\$\w+(?:/[^/\s]+)*)\s+ge\s+(.+)', condition)
        if gte_match:
            path = gte_match.group(1).strip()
            value = gte_match.group(2).strip()
            field = self._parse_path(path, context_var)
            parsed_value = self._parse_value(value)
            
            return {field: {"$gte": parsed_value}}
        
        # Check for less than condition: $item/field < value or $item/field lt value
        lt_match = re.search(r'(\$\w+(?:/[^/\s]+)*)\s*<\s*(.+)', condition) or re.search(r'(\$\w+(?:/[^/\s]+)*)\s+lt\s+(.+)', condition)
        if lt_match:
            path = lt_match.group(1).strip()
            value = lt_match.group(2).strip()
            field = self._parse_path(path, context_var)
            parsed_value = self._parse_value(value)
            
            return {field: {"$lt": parsed_value}}
        
        # Check for less than or equal condition: $item/field <= value or $item/field le value
        lte_match = re.search(r'(\$\w+(?:/[^/\s]+)*)\s*<=\s*(.+)', condition) or re.search(r'(\$\w+(?:/[^/\s]+)*)\s+le\s+(.+)', condition)
        if lte_match:
            path = lte_match.group(1).strip()
            value = lte_match.group(2).strip()
            field = self._parse_path(path, context_var)
            parsed_value = self._parse_value(value)
            
            return {field: {"$lte": parsed_value}}
        
        # Check for contains condition: contains($item/field, "substring")
        contains_match = re.search(r'contains\s*\(\s*(\$\w+(?:/[^/\s,]+)*)\s*,\s*(.+?)\s*\)', condition)
        if contains_match:
            path = contains_match.group(1).strip()
            substring = contains_match.group(2).strip()
            field = self._parse_path(path, context_var)
            parsed_substring = self._parse_value(substring)
            
            if isinstance(parsed_substring, str):
                # Use regex for string contains
                return {field: {"$regex": parsed_substring, "$options": "i"}}
            else:
                raise ValueError(f"contains() operator can only be used with strings, got: {substring}")
        
        # Check for starts-with condition: starts-with($item/field, "prefix")
        starts_with_match = re.search(r'starts\-with\s*\(\s*(\$\w+(?:/[^/\s,]+)*)\s*,\s*(.+?)\s*\)', condition)
        if starts_with_match:
            path = starts_with_match.group(1).strip()
            prefix = starts_with_match.group(2).strip()
            field = self._parse_path(path, context_var)
            parsed_prefix = self._parse_value(prefix)
            
            if isinstance(parsed_prefix, str):
                # Use regex for starts-with
                return {field: {"$regex": f"^{re.escape(parsed_prefix)}", "$options": "i"}}
            else:
                raise ValueError(f"starts-with() operator can only be used with strings, got: {prefix}")
        
        # Check for ends-with condition: ends-with($item/field, "suffix")
        ends_with_match = re.search(r'ends\-with\s*\(\s*(\$\w+(?:/[^/\s,]+)*)\s*,\s*(.+?)\s*\)', condition)
        if ends_with_match:
            path = ends_with_match.group(1).strip()
            suffix = ends_with_match.group(2).strip()
            field = self._parse_path(path, context_var)
            parsed_suffix = self._parse_value(suffix)
            
            if isinstance(parsed_suffix, str):
                # Use regex for ends-with
                return {field: {"$regex": f"{re.escape(parsed_suffix)}$", "$options": "i"}}
            else:
                raise ValueError(f"ends-with() operator can only be used with strings, got: {suffix}")
        
        # Check for exists condition: exists($item/field)
        exists_match = re.search(r'exists\s*\(\s*(\$\w+(?:/[^/\s]+)*)\s*\)', condition)
        if exists_match:
            path = exists_match.group(1).strip()
            field = self._parse_path(path, context_var)
            
            return {field: {"$exists": True}}
        
        # Check for not exists condition: not(exists($item/field))
        not_exists_match = re.search(r'not\s*\(\s*exists\s*\(\s*(\$\w+(?:/[^/\s]+)*)\s*\)\s*\)', condition)
        if not_exists_match:
            path = not_exists_match.group(1).strip()
            field = self._parse_path(path, context_var)
            
            return {field: {"$exists": False}}
        
        raise ValueError(f"Unsupported condition: {condition}")
    
    def _parse_path(self, path, context_var):
        """Parse an XPath-like path into a MongoDB field path"""
        
        # Replace variable with empty prefix
        if path.startswith(f"${context_var}/"):
            path = path[len(f"${context_var}/"):]
        elif path == f"${context_var}":
            return "_id"  # Default to _id for the document itself
        
        # Convert XPath notation to dot notation
        return path.replace('/', '.')
    
    def _parse_value(self, value_str):
        """Parse a value string into the appropriate type"""
        
        value_str = value_str.strip()
        
        # String literal
        if (value_str.startswith('"') and value_str.endswith('"')) or (value_str.startswith("'") and value_str.endswith("'")):
            return value_str[1:-1]
        
        # Number
        if value_str.isdigit():
            return int(value_str)
        
        # Float
        try:
            if re.match(r'^[0-9]+\.[0-9]+$', value_str):
                return float(value_str)
        except ValueError:
            pass
        
        # Boolean
        if value_str.lower() == 'true':
            return True
        if value_str.lower() == 'false':
            return False
        
        # Null/None
        if value_str.lower() in ('null', 'none'):
            return None
        
        # Function call or complex expression
        if '(' in value_str:
            # This is a simplified handling - for complex expressions,
            # you might need more sophisticated parsing
            return value_str
        
        return value_str
    
    def _parse_order_by(self, order_by_clause, context_var):
        """Parse an order by clause into a MongoDB sort specification"""
        
        sort_spec = {}
        parts = re.split(r',\s*', order_by_clause)
        
        for part in parts:
            # Check for descending order
            if re.search(r'descending$', part, re.IGNORECASE):
                direction = -1
                part = re.sub(r'\s+descending$', '', part, flags=re.IGNORECASE)
            else:
                direction = 1
                # Remove potential "ascending" keyword
                part = re.sub(r'\s+ascending$', '', part, flags=re.IGNORECASE)
            
            # Parse the field path
            field = self._parse_path(part.strip(), context_var)
            sort_spec[field] = direction
        
        return sort_spec
    
    def _parse_return_clause(self, return_clause, context_var):
        """Parse a return clause into a MongoDB projection"""
        
        # Simple projection based on direct field references
        projection = {}
        
        # Look for explicitly mentioned fields
        field_pattern = r'\$' + context_var + r'/([^/\s,]+)'
        fields = re.findall(field_pattern, return_clause)
        
        for field in fields:
            projection[field] = 1
        
        # If specific fields are listed, exclude _id unless explicitly included
        if fields and '_id' not in projection:
            projection['_id'] = 0
        
        return projection


def main():
    parser = argparse.ArgumentParser(description='Convert XQuery CRUD statements to MongoDB operations')
    parser.add_argument('file', nargs='?', type=argparse.FileType('r'), default=sys.stdin,
                        help='Input file with XQuery statements (default: stdin)')
    parser.add_argument('-o', '--output', type=argparse.FileType('w'), default=sys.stdout,
                        help='Output file for MongoDB operations (default: stdout)')
    
    args = parser.parse_args()
    
    converter = XQueryToMongoCRUDConverter()
    
    for line in args.file:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        try:
            result = converter.parse_xquery(line)
            args.output.write(json.dumps(result, indent=2) + '\n')
        except Exception as e:
            print(f"Error processing line: {line}", file=sys.stderr)
            print(f"Error: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()




