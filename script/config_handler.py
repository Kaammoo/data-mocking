from utilities import read_config_yamle, tables, read_file


def handle_config_changes(default_models=None):
    change = input("Do you need to change anything in configs? (y/yes or n/no): ")
    conf = read_config_yamle("script/configs.yml")
    if change.lower() in ["y", "yes"]:

        changes = {}
        models = [str(i) for i in range(1, 4)]
        print(read_file("script/configs.yml"))

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

        # Merge changes with existing configuration
    elif change.lower() in ["n", "no"]:
        changes = {}
        run_all = input("Do you want to run all models? (y/yes or n/no): ").lower()
        if run_all in ["y", "yes"]:
            models = [str(i) for i in range(1, 4)]
        elif run_all in ["n", "no"]:
            tables()
            models = input("Enter model numbers separated by space: ").split()
        else:
            print("Invalid input. Please enter y/yes or n/no.")
            return False, False
    else:
        print("Invalid input. Please enter y/yes or n/no.")
        return False, False
    
    
    conf.update(changes)
    # Return only the required keys
    return {
        k: conf[k]
        for k in (
            "max_field_count",
            "min_field_count",
            "max_field_size",
            "min_field_size",
            "duration",
            "min_users_per_community",
            "max_users_per_community",
            "devices_weights",
            "community_weights",
            "product_weights",
        )
    }, models
