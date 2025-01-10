# Function to get the main asset query with basic nested_limit
def get_query(asset_type_id, paginate, nested_offset=0, nested_limit=50):
    return f"""
    query Assets($limit: Int!) {{
        assets(
            where: {{ type: {{ id: {{ eq: "{asset_type_id}" }} }} id:{{gt:{paginate}}} }}
            limit: $limit
        ) {{
            id
            fullName
            displayName
            modifiedOn
            modifiedBy{{ 
                fullName
            }}
            createdOn
            createdBy{{
                fullName
            }}
            status{{
                name
            }}
            type {{
                name
            }}
            domain {{
                name
                parent {{
                    name
                }}
            }}
            stringAttributes (offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                stringValue
            }}
            multiValueAttributes (offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                stringValues
            }}
            numericAttributes (offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                numericValue
            }}
            dateAttributes (offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                dateValue
            }}
            booleanAttributes (offset: {nested_offset}, limit: {nested_limit}) {{
                type {{
                    name
                }}
                booleanValue
            }}
            outgoingRelations (offset: {nested_offset}, limit: {nested_limit}) {{
                target {{
                    id
                    fullName
                    displayName
                    type {{
                        name
                    }}
                }}
                type {{
                    role
                }}
            }}
            incomingRelations (offset: {nested_offset}, limit: {nested_limit}) {{
                source {{
                    id
                    fullName
                    displayName
                    type {{
                        name
                    }}
                }}
                type {{
                    corole
                }}
            }}
            responsibilities (offset: {nested_offset}, limit: {nested_limit}) {{
                role {{
                    name
                }}
                user {{
                    fullName
                    email
                }}
            }}
        }}
    }}
    """

def get_nested_query(asset_type_id, asset_id, field_name, nested_limit=20000):
    """
    Generate a query for fetching a specific nested field with high limits.
    Uses main_limit=1 to fetch single asset and nested_limit for the nested fields.
    """
    # Base query structure with limit=1 to ensure we only get one asset
    base_query = f"""
    query Assets {{
        assets(
            where: {{ 
                type: {{ id: {{ eq: "{asset_type_id}" }} }}
                id: {{ eq: "{asset_id}" }}
            }}
            limit: 1
        ) {{
            id
    """

    # Field-specific query parts
    field_queries = {
        'stringAttributes': f"""
            stringAttributes(limit: {nested_limit}) {{
                type {{
                    name
                }}
                stringValue
            }}
        """,
        'multiValueAttributes': f"""
            multiValueAttributes(limit: {nested_limit}) {{
                type {{
                    name
                }}
                stringValues
            }}
        """,
        'numericAttributes': f"""
            numericAttributes(limit: {nested_limit}) {{
                type {{
                    name
                }}
                numericValue
            }}
        """,
        'dateAttributes': f"""
            dateAttributes(limit: {nested_limit}) {{
                type {{
                    name
                }}
                dateValue
            }}
        """,
        'booleanAttributes': f"""
            booleanAttributes(limit: {nested_limit}) {{
                type {{
                    name
                }}
                booleanValue
            }}
        """,
        'outgoingRelations': f"""
            outgoingRelations(limit: {nested_limit}) {{
                target {{
                    id
                    fullName
                    displayName
                    type {{
                        name
                    }}
                }}
                type {{
                    role
                }}
            }}
        """,
        'incomingRelations': f"""
            incomingRelations(limit: {nested_limit}) {{
                source {{
                    id
                    fullName
                    displayName
                    type {{
                        name
                    }}
                }}
                type {{
                    corole
                }}
            }}
        """,
        'responsibilities': f"""
            responsibilities(limit: {nested_limit}) {{
                role {{
                    name
                }}
                user {{
                    fullName
                    email
                }}
            }}
        """
    }

    if field_name not in field_queries:
        raise ValueError(f"Unsupported field name: {field_name}")

    # Construct the complete query
    complete_query = base_query + field_queries[field_name] + "}}"

    return complete_query