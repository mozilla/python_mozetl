import csv
import pdb


def write_csv(dataframe, path):
    """ Write a dataframe to local disk. """

    # pdb.set_trace()
    with open(path, 'wb') as fout:
        writer = csv.writer(fout)
        writer.writerow(dataframe.columns)
        for row in dataframe.toLocalIterator():
            row = [unicode(s).encode('utf-8') for s in row]
            writer.writerow(row)
