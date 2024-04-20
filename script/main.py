from DataMocking import DataMocking
from config_handler import handle_config_changes
def main():
    changes, models = handle_config_changes()
    print(f"{changes = }")
    print(f"{models = }")
    project = DataMocking(changes, models)
    project.run(changes)
if __name__ == "__main__":
    main()
