
# From: https://github.com/yanne/pypcaxis

import re
from itertools import product
from operator import mul


class Dimension(object):

    def __init__(self, title, values):
        self.title = title
        self.values = values

    def __len__(self):
        return len(self.values)

    def __repr__(self):
        return "%s(%d)" % (self.title, len(self))


class Table(object):

    def __init__(self):
        self.dimensions = []
        self.data = None

    def __repr__(self):
        return "PCAxis(dimensions=%s)" % (str(self.dimensions))

    def add_dimension(self, dimension):
        self.dimensions.append(dimension)

    def get_by(self, title, value):
        #FIXME does not work!!!
        title_index = [dim.title for dim in self.dimensions].index(title)
        dims = [dim.values for dim in self.dimensions]
        dims[title_index] = [value]
        table = Table()
        table.dimension = [d for d in self.dimensions if d.title != title]
        table.data = [self.get(*criteria) for criteria in reversed(list(product(*dims)))]
        return table

    def get(self, *criteria):
        dim_lenghts = [len(dim) for dim in self.dimensions]
        dim_indices = [dim.values.index(c) for (dim, c)
                       in zip(self.dimensions, criteria)]
        return self.data[sum(reduce(mul, dim_lenghts[i + 1:], 1) * index for i, index in enumerate(dim_indices))]


def parse(data):
    data = read_data(data)
    value_regex = re.compile(r'VALUES\(\"(.*)\"\)')
    table = Table()
    for item in data:
        if not item:
            continue
        name, values = [t.strip() for t in item.split('=', 1)]
        value_match = value_regex.match(name)
        if value_match:
            title = value_match.group(1)
            table.add_dimension(create_dimension(title, values))
        if name == 'DATA':
            table.data = [i.strip() for i in values.split(' ')]
    return table


def read_data(data):
    return [t.strip() for t in
            data.decode('ISO-8859-1').split(';')]


def create_dimension(title, values):
    # values are defined like: "foo","bar","zap"
    values = values.replace('\r\n', '')[1:-1].split('","')
    return Dimension(title, values)


"""
if __name__ == '__main__':
    table = parse('examples/tulot.px')
    print table.get('2008', 'Tuusula - Tusby', 'Veronalaiset tulot, mediaani')
    print table.get('2009', 'Tuusula - Tusby', 'Veronalaiset tulot, mediaani')
    print table.get('2007', u'Hyvink\xe4\xe4 - Hyvinge', 'Tulonsaajia')
    print table.get_by('Vuosi', '2007').get(u'Hyvink\xe4\xe4 - Hyvinge', 'Tulonsaajia')
    print table.get('2008', 'Tuusula - Tusby', 'Veronalaiset tulot, mediaani')
    table = parse('examples/vaalit.px')
    print table.get('Uudenmaan vaalipiiri', 'VIHR', u'Yhteens\xe4', u'78 vuotta')
"""
