import csv


def write_csv(dataframe, path):
    """ Write a dataframe to local disk. """

    with open(path, 'wb') as fout:
        writer = csv.writer(fout)
        writer.writerow(dataframe.columns)
        for row in dataframe.toLocalIterator():
            row = [unicode(s).encode('utf-8') for s in row]
            writer.writerow(row)
