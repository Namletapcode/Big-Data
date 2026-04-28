import os
import json


def get_state(filepath, key, default_value=0):
    if not os.path.exists(filepath):
        return default_value
    
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            data = json.load(file)

            return data.get(key, default_value)
        
    except:
        return default_value
    
def update_state(filepath, key, value):
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    data = {}
    
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as file:
                data = json.load(file)
        
        except:
            pass

    data[key] = value

    with open(filepath, 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4)
