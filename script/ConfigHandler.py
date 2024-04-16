from Utilities import read_config

def handle_config_changes(default_models=None):
    changes = {}
    models = default_models if default_models else [str(i) for i in range(1, 12)] 

    while True:
        print("These are your arguments:")
        change_input = input(
            "Enter the change you want to make (key=new_value), or enter 'done' to finish: "
        )
        if change_input.lower() == "done":
            break
        elif change_input.lower() == "not all":
            models = input("Enter model numbers separated by space: ").split()
        else:
            key, value = change_input.split("=")
            changes[key.strip()] = value.strip()
    
    conf = read_config("script/Configs.py")
    
    # Merge changes with existing configuration
    conf.update(changes)
    
    # Return only the required keys
    return {k: conf[k] for k in (
        'max_field_count', 'min_field_count', 'max_field_size', 'min_field_size',
        'duration', 'min_users_per_community1', 'max_users_per_community1',
        'devices_weights', 'community_weights', 'product_weights'
    )}, models

