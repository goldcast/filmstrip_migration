import os
import shutil

output_directory = "downloads/{}/{}"


def cleanup_directory(project_id, content_id):
    output_dir = output_directory.format(project_id, content_id)
    # Check if the directory exists
    if not os.path.exists(output_dir):
        print(f"The directory {output_dir} does not exist.")
        return

    # Iterate over the items in the directory
    for item in os.listdir(output_dir):
        item_path = os.path.join(output_dir, item)

        # If it's a file, delete it
        if os.path.isfile(item_path) or os.path.islink(item_path):
            os.unlink(item_path)

        # If it's a directory, delete it and all its contents
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)

    print(f"Directory {output_dir} has been cleaned up.")