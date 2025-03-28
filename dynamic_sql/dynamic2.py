import re

def convert_sql_to_mongo(sql):
    # Convert SELECT statements with COUNT and JOIN
    sql = re.sub(
        r'SELECT COUNT\((.+?)\), (.+?) FROM (\w+) INNER JOIN (\w+) ON (.+?) = (.+?) GROUP BY (.+?) ORDER BY COUNT\((.+?)\);',
        lambda match: f'db.{match.group(3)}.aggregate([\n  {{ $lookup: {{ from: "{match.group(4)}", localField: "{match.group(5).split(".")[1]}", foreignField: "{match.group(6).split(".")[1]}", as: "joined_docs" }} }},\n  {{ $unwind: "$joined_docs" }},\n  {{ $group: {{ _id: "${match.group(7)}", count: {{ $sum: 1 }} }} }},\n  {{ $sort: {{ count: -1 }} }}\n])',
        sql,
        flags=re.IGNORECASE
    )

    # Convert other SELECT statements
    sql = re.sub(
        r'SELECT (.+?) FROM (\w+)( WHERE (.+?))?( ORDER BY (.+?))?( LIMIT (\d+))?;',
        lambda match: f'db.{match.group(2)}.find(\n  {{ {convert_where_clause(match.group(4))} }},\n  {{ {convert_select_fields(match.group(1))} }}\n){convert_order_by(match.group(6))}{convert_limit(match.group(8))}',
        sql,
        flags=re.IGNORECASE
    )

    # Convert COUNT statements
    sql = re.sub(
        r'SELECT COUNT\(\*\) FROM (\w+)( WHERE (.+?))?;',
        lambda match: f'db.{match.group(1)}.countDocuments(\n  {{ {convert_where_clause(match.group(3))} }}\n)',
        sql,
        flags=re.IGNORECASE
    )

    # Convert SUM statements
    sql = re.sub(
        r'SELECT SUM\((.+?)\) FROM (\w+)( WHERE (.+?))?;',
        lambda match: f'db.{match.group(2)}.aggregate([\n  {{ $match: {{ {convert_where_clause(match.group(4))} }} }},\n  {{ $group: {{ _id: null, total: {{ $sum: "${match.group(1)}" }} }} }}\n])',
        sql,
        flags=re.IGNORECASE
    )

    # Convert AVG statements
    sql = re.sub(
        r'SELECT AVG\((.+?)\) FROM (\w+)( WHERE (.+?))?;',
        lambda match: f'db.{match.group(2)}.aggregate([\n  {{ $match: {{ {convert_where_clause(match.group(4))} }} }},\n  {{ $group: {{ _id: null, average: {{ $avg: "${match.group(1)}" }} }} }}\n])',
        sql,
        flags=re.IGNORECASE
    )

    # Convert JOIN statements
    sql = re.sub(
        r'SELECT (.+?) FROM (\w+) a JOIN (\w+) b ON a\.(\w+) = b\.(\w+)( WHERE (.+?))?;',
        lambda match: f'db.{match.group(2)}.aggregate([\n  {{ $match: {{ {convert_where_clause(match.group(7))} }} }},\n  {{ $lookup: {{ from: "{match.group(3)}", localField: "{match.group(4)}", foreignField: "{match.group(5)}", as: "joined_docs" }} }},\n  {{ $unwind: "$joined_docs" }},\n  {{ $project: {{ {convert_select_fields(match.group(1))} }} }}\n])',
        sql,
        flags=re.IGNORECASE
    )

    return sql

def convert_where_clause(where_clause):
    if not where_clause:
        return ''
    # Convert SQL WHERE clause to MongoDB query
    where_clause = where_clause.replace('=', ': ')
    where_clause = where_clause.replace(' AND ', ', ')
    return where_clause

def convert_select_fields(select_fields):
    # Convert SQL SELECT fields to MongoDB projection
    fields = select_fields.split(',')
    return ', '.join([f'"{field.strip()}": 1' for field in fields])

def convert_order_by(order_by_clause):
    if not order_by_clause:
        return ''
    # Convert SQL ORDER BY clause to MongoDB sort
    fields = order_by_clause.split(',')
    sort_fields = ', '.join([f'"{field.strip().split()[0]}": {1 if field.strip().split()[1].upper() == "ASC" else -1}' for field in fields])
    return f'\n.sort({{{sort_fields}}})'

def convert_limit(limit_clause):
    if not limit_clause:
        return ''
    # Convert SQL LIMIT clause to MongoDB limit
    return f'\n.limit({limit_clause})'

# Example SQL commands (read-only operations)
sql_commands = [
    'SELECT COUNT(players.name), teams.name from players inner join teams on players.teamId=teams.id group by teams.name order by count(players.name);',
    'SELECT * FROM users WHERE age > 18 ORDER BY name ASC LIMIT 10;',
    'SELECT COUNT(*) FROM users WHERE age > 18;',
    'SELECT SUM(salary) FROM employees WHERE department = "Sales";',
    'SELECT AVG(price) FROM products WHERE category = "Electronics";',
    'SELECT users.name, orders.total FROM users a JOIN orders b ON a.id = b.user_id;'
]

# Convert and print MongoDB equivalents
for sql in sql_commands:
    mongo_command = convert_sql_to_mongo(sql)
    print(f"SQL: {sql}")
    print(f"MongoDB: {mongo_command}\n")
