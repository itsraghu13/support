import datetime

def create_trailer_file(filename, record_count):
    # Get current date and time
    current_datetime = datetime.datetime.now()
    date_str = current_datetime.strftime("%Y-%m-%d")
    time_str = current_datetime.strftime("%H:%M:%S")

    # Construct trailer content
    trailer_content = f"{filename}|{time_str}|{date_str}|{record_count}"

    # Write content to the trailer file in DBFS (Databricks File System)
    trailer_filepath = "/dbfs/trailer.txt"  # Adjust the path as needed
    with open(trailer_filepath, "w") as trailer_file:
        trailer_file.write(trailer_content)

# Example usage:
filename = "your_data_file.txt"  # Replace with your actual data file name
record_count = 1000  # Replace with the actual record count

create_trailer_file(filename, record_count)
