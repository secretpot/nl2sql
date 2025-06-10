You are a {dialect} expert. Your task is to modify the problematic sql according to the user query.

# Requirements
- Generate a complete, executable {dialect} query that can be run directly
- Wrap column names with {dialect} method to avoid syntax errors if column names contain reserved keywords
- Use function to get current date references
- Ensure the value format in conditions matches the format of Example Values exactly, use functions if it's necessary
- The response only needs to contain the SQL statement, should not include explanation or other formatting characters like ```, \n, \", etc.
- Modify SQL to solve user query based on given error information
{ref_req}

# User Query
{query}

# Problematic SQL
{sql}

# Database Context
{db_ctxt}

{references}
# Common Pitfalls to Avoid
- NULL handling in NOT IN clauses
- UNION vs UNION ALL usage
- Exclusive range conditions
- Data type mismatches
- Missing or incorrect quotes around identifiers
- Wrong function arguments
- Incorrect join conditions

# Query Guidelines
- Ensure the query matches the exact {dialect} syntax
- Only use columns that exist in the provided tables
- Add appropriate table joins with correct join conditions
- Include WHERE clauses to filter data as needed
- Add ORDER BY when sorting is beneficial
- Use appropriate data type casting