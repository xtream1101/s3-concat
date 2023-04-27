import concurrent.futures
import logging
import re

logger = logging.getLogger(__name__)

# S3 multi-part upload parts must be larger than 5mb
KB = 1024
MB = KB**2
GB = KB**3
TB = KB**4
MIN_S3_SIZE = 5 * MB


def _thread_run(item, callback):
    for _ in range(3):
        # re try 3 times before giving up
        try:
            response = callback(item)
            return response
        except Exception:
            logger.exception("Retry failed batch of: {}".format(item))


def _threads(num_threads, data, callback):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:  # noqa: E501
        futures = (
            executor.submit(_thread_run, d, callback)
            for d in data
        )

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if not result:
                raise Exception("no response gotten from callback")

            results.append(result)

    return results


def _create_s3_client(session, s3_client_kwargs=None):
    if s3_client_kwargs is None:
        s3_client_kwargs = {}
    return session.client('s3', **s3_client_kwargs)


def _chunk_by_size(file_list, min_file_size):
    """Split list by size of file

    Arguments:
        file_list {list} -- List of tuples as (<filename>, <file_size>)
        min_file_size {int} -- Min part file size in bytes

    Returns:
        list -- Each list of files is the min file size
    """
    grouped_list = []
    current_list = []
    current_size = 0
    current_index = 1
    for p in file_list:
        if min_file_size is not None:
            # If the current file is already larger then the min file size,
            # just add it to the list on its own
            if p[1] > min_file_size:
                grouped_list.append((current_index, [p]))
                current_index += 1
                continue

            # If multiple files have hit the min size, add them to the list
            if current_size > min_file_size:
                grouped_list.append((current_index, current_list))
                current_list = []
                current_size = 0
                current_index += 1

        current_size += p[1]
        current_list.append(p)

    # Get anything left over that did not add up to the min size
    if current_size != 0:
        grouped_list.append((current_index, current_list))

    return grouped_list


def _convert_to_bytes(value):
    """Convert the input value to bytes

    Arguments:
        value {string} -- Value and size of the input with no spaces

    Returns:
        float -- The value converted to bytes as a float

    Raises:
        ValueError -- if the input value is not a valid type to convert
    """
    if value is None:
        return None
    value = value.strip()
    sizes = {'KB': 1024,
             'MB': 1024**2,
             'GB': 1024**3,
             'TB': 1024**4,
             }
    if value[-2:].upper() in sizes:
        return float(value[:-2].strip()) * sizes[value[-2:].upper()]
    elif re.match(r'^\d+(\.\d+)?$', value):
        return float(value)
    elif re.match(r'^\d+(\.\d+)?\s?B$', value):
        return float(value[:-1])
    else:
        raise ValueError("Value {} is not a valid size".format(value))
