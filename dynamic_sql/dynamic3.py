import sqlparse
import re

class SQLToMongoConverter:
    def __init__(self):
        self.operators_map = {
            '=': '$eq',
            '!=': '$ne',
            '>': '$gt',
            '>=': '$gte',
            '<': '$lt',
            '<=': '$lte',
            'LIKE': '$regex',
            'IN': '$in',
            'NOT IN': '$nin'
        }

    def convert_where_clause(self, where_clause):
        if not where_clause:
            return {}

        # More sophisticated where clause parsing
        conditions = {}
        
        # Basic parsing of simple conditions
        for condition in re.findall(r'(\w+)\s*(=|>|<|>=|<=|!=)\s*(["\']?.*?["\']?)\s*(AND|OR)?', where_clause):
            field, operator, value, logical_op = condition
            
            # Remove quotes if present
            value = value.strip("'\"")
            
            # Handle type conversion
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass

            # Convert operator
            mongo_op = self.operators_map.get(operator, operator)
            
            if mongo_op == '$regex':
                # Handle LIKE with basic wildcard conversion
                value = value.replace('%', '.*')

            conditions[field] = {mongo_op: value}

        return conditions

    def convert_select(self, parsed_sql):
        # Extract key components from parsed SQL
        tables = [token.value for token in parsed_sql.tokens if isinstance(token, sqlparse.sql.Identifier) and token.ttype is None]
        
        # Handle projection
        select_tokens = [token for token in parsed_sql.tokens if isinstance(token, sqlparse.sql.IdentifierList)]
        projections = {}
        if select_tokens:
            for item in select_tokens[0].tokens:
                if isinstance(item, sqlparse.sql.Identifier):
                    projections[item.value] = 1

        # Handle WHERE clause
        where_clause = None
        for token in parsed_sql.tokens:
            if isinstance(token, sqlparse.sql.Where):
                where_clause = str(token).replace('WHERE', '').strip()

        # Construct MongoDB query
        query = {
            'collection': tables[0],
            'find_params': {
                'filter': self.convert_where_clause(where_clause) if where_clause else {},
                'projection': projections if projections else None
            }
        }

        return query

    def convert(self, sql_query):
        # Parse the SQL query
        parsed = sqlparse.parse(sql_query)[0]
        
        # Determine query type and convert
        if parsed.get_type() == 'SELECT':
            return self.convert_select(parsed)
        
        return None

# Example usage
converter = SQLToMongoConverter()

# Test queries
sql_queries = [
    'SELECT name, age FROM users WHERE age > 25 AND city = "New York"',
    'SELECT * FROM products WHERE price < 100',
    'SELECT username FROM accounts WHERE status LIKE "%active%"'
]

for query in sql_queries:
    mongo_equivalent = converter.convert(query)
    print(f"SQL: {query}")
    print(f"MongoDB: {mongo_equivalent}\n")
