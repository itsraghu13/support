import concurrent.futures
import delta

def resolve_conflicts_optimistically(table_path):
    """
    Resolves conflicts in the given Delta Lake table optimistically.

    Args:
        table_path: The path to the Delta Lake table.
    """

    # Record the starting table version.
    starting_version = delta.get_current_version(table_path)

    # Record reads/writes.
    reads = []
    writes = []

    # Attempt a commit.
    commit = delta.commit(table_path, reads, writes)

    # If someone else wins, check whether anything you read has changed.
    if commit.state != delta.DeltaCommitState.COMMITTED:
        new_version = commit.version
        delta_log = delta.read_delta_log(table_path)
        for entry in delta_log.entries:
            if entry.version > new_version:
                # Something has changed, so we need to retry the commit.
                return resolve_conflicts_optimistically(table_path)

        # Nothing has changed, so we can proceed with the commit.
        return commit


def process_run(item):
    # Your existing code for processing each run
    queryActivity_URL = "https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/"
    headers = {'Authorization': 'Bearer ' + access_token}
    # ... (rest of the code for processing a single run)


if len(run_list) > 0:
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # You can adjust max_workers to control the number of parallel threads
        futures = [executor.submit(process_run, item) for item in run_list]

        # Wait for all threads to finish
        concurrent.futures.wait(futures)

else:
    raise Exception('get_pipeline_list returned empty')

