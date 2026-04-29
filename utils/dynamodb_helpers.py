"""Helper functions for DynamoDB data type conversions"""


def python_to_dynamodb(obj):
    """
    Convert Python object to DynamoDB format
    Handles nested dicts, lists, strings, numbers, booleans
    """
    if obj is None:
        return {"NULL": True}
    elif isinstance(obj, bool):
        return {"BOOL": obj}
    elif isinstance(obj, (int, float)):
        return {"N": str(obj)}
    elif isinstance(obj, str):
        return {"S": obj}
    elif isinstance(obj, list):
        return {"L": [python_to_dynamodb(item) for item in obj]}
    elif isinstance(obj, dict):
        return {"M": {k: python_to_dynamodb(v) for k, v in obj.items()}}
    else:
        # Fallback to string
        return {"S": str(obj)}


def dynamodb_to_python(obj):
    """
    Convert DynamoDB format to Python object
    """
    if not isinstance(obj, dict):
        return obj
    
    if "S" in obj:
        return obj["S"]
    elif "N" in obj:
        num = obj["N"]
        # Try to convert to int if possible, otherwise float
        try:
            if '.' in num:
                return float(num)
            return int(num)
        except:
            return float(num)
    elif "BOOL" in obj:
        return obj["BOOL"]
    elif "NULL" in obj:
        return None
    elif "L" in obj:
        return [dynamodb_to_python(item) for item in obj["L"]]
    elif "SS" in obj:
        return [str(item) for item in obj["SS"]]
    elif "NS" in obj:
        result = []
        for num in obj["NS"]:
            try:
                if "." in num:
                    result.append(float(num))
                else:
                    result.append(int(num))
            except Exception:
                result.append(float(num))
        return result
    elif "M" in obj:
        return {k: dynamodb_to_python(v) for k, v in obj["M"].items()}
    else:
        return obj
