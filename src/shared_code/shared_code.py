import csv
import itertools
import shutil
import uuid


def read_csv_columns(in_filename, cols, out_file=None):
    """

    """
    holder = list()
    with open(in_filename, 'r') as f:
        reader = csv.reader(f, delimiter=' ')
        for row in reader:
            holder.append(list(row[i] for i in cols))

    holder = unique_everseen(holder)
    


def unique_sort_with_replacement(in_filename):
    """

    """
    name = str(uuid.uuid4())
    with open(in_filename, 'r') as f:
        with open(name, 'w') as holder:
            holder.writelines(unique_everseen(f))

    shutil.move(name, in_filename)


def unique_everseen(iterable, key=None):
    """
    List unique elements, preserving order. Remember all elements ever seen.
    Taken from: https://docs.python.org/3.7/library/itertools.html
    """
    seen = set()
    seen_add = seen.add
    if key is None:
        for element in itertools.filterfalse(seen.__contains__, iterable):
            seen_add(element)
            yield element
    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                seen_add(k)
                yield element
